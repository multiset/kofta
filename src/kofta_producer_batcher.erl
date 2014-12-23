-module(kofta_producer_batcher).

-behaviour(gen_server).

-export([
    send/3,
    name/2
]).

-export([start_link/2]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(st, {
    clients,
    msgs,
    last_batch,
    max_latency=100,
    host,
    port
}).


-spec send(TopicName, Partition, [{Key, Value}]) -> {ok, Offset} | Error when
    TopicName :: binary(),
    Partition :: integer(),
    Key :: binary() | null,
    Value :: binary() | null,
    Offset :: integer(),
    Error :: {error, any()}.

send(Topic, Partition, KVs) ->
    case kofta_metadata:get_leader(Topic, Partition) of
        {ok, {Host, Port}} ->
            gen_server:call(name(Host, Port), {msg, Topic, Partition, KVs});
        {error, Reason} ->
            {error, Reason}
    end.


start_link(Host, Port) ->
    gen_server:start_link({local, name(Host, Port)}, ?MODULE, [Host, Port], []).


init([Host, Port]) ->
    State = #st{
        clients=dict:new(),
        last_batch=now(),
        msgs=dict:new(),
        host=Host,
        port=Port
    },
    {ok, State}.


handle_call({msg, Topic, Partition, KVs}, From, State0) ->
    #st{clients=Clients, msgs=Msgs} = State0,
    State1 = State0#st{
        clients=dict:append({Topic, Partition}, From, Clients),
        msgs=dict:append_list({Topic, Partition}, KVs, Msgs)
    },
    format_return(noreply, State1).


handle_cast(_Msg, State) ->
    format_return(noreply, State).


handle_info(timeout, State) ->
    format_return(noreply, State).


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    format_return(ok, State).


-spec format_return(Type, State) -> {Type, State} | {Type, State, Timeout} when
    Type :: atom(),
    State :: #st{},
    Timeout :: integer().

format_return(Type, State) ->
    #st{
        clients=Clients
    } = State,
    % is_empty isn't in r16
    case dict:size(Clients) of
        0 ->
            {Type, State};
        _ ->
            NewState = maybe_make_request(State),
            {Type, NewState, get_timeout(NewState)}
    end.


-spec maybe_make_request(State) -> State when
    State :: #st{}.

maybe_make_request(State) ->
    #st{last_batch=LastBatch, max_latency=MaxLatency} = State,
    case timer:now_diff(os:timestamp(), LastBatch)/1000 of
        Diff when Diff >= MaxLatency ->
            make_request(State);
        _ ->
            State
    end.


-spec make_request(State) -> State when
    State :: #st{}.

make_request(State) ->
    #st{
        clients=ClientDict,
        msgs=MsgDict,
        host=Host,
        port=Port
    } = State,

    % This bit here formats the accumulated messages to be encoded for the
    % kafka binary protocol
    TopicParts = lists:sort(dict:fetch_keys(MsgDict)),
    FinalAcc = lists:foldl(fun({Topic, PartID}, Acc) ->
        Msgs = dict:fetch({Topic, PartID}, MsgDict),
        case Acc of
            {_Nil, [], []} ->
                {Topic, [{PartID, Msgs}], []};
            {Topic, PartAcc, TopicAcc} ->
                {Topic, [{PartID, Msgs}|PartAcc], TopicAcc};
            {OldTopic, PartAcc, TopicAcc} ->
                {Topic, [{PartID, Msgs}], [{OldTopic, PartAcc}|TopicAcc]}
        end
    end, {nil, [], []}, TopicParts),
    RequestData = case FinalAcc of
        {_Topic, [], TopicAcc} ->
            TopicAcc;
        {Topic, PartAcc, TopicAcc} ->
            [{Topic, PartAcc}|TopicAcc]
    end,

    ProduceBinBody = kofta_encode:array(fun({Topic, Partitions}) ->
        PartBin = kofta_encode:array(fun({PartID, Msgs}) ->
            MsgSet = kofta_encode:message_set(Msgs),
            [<<PartID:32/big-signed-integer,
             (iolist_size(MsgSet)):32/big-signed-integer>>,
             MsgSet]
        end, Partitions),
        [kofta_encode:string(Topic), PartBin]
    end, RequestData),
    Header = <<1:16/big-signed-integer, 10000:32/big-signed-integer>>,
    BinRequest = kofta_encode:request(0, 0, 0, <<>>, [Header,ProduceBinBody]),

    {ok, Response} = kofta_connection:request(Host, Port, BinRequest),
    {_Request, Rest0} = kofta_decode:request(Response),
    {Body, <<>>} = kofta_decode:array(fun(Binary0) ->
        {TopicName, IRest0} = kofta_decode:string(Binary0),
        {PartResps, IRest2} = kofta_decode:array(fun(Binary1) ->
            <<PartitionID:32/big-signed-integer,
              ErrorCode:16/big-signed-integer,
              Offset:64/big-signed-integer,
              IRest1/binary>> = Binary1,
            {{PartitionID, ErrorCode, Offset}, IRest1}
        end, IRest0),
        {{TopicName, PartResps}, IRest2}
    end, Rest0),

    Responses = lists:foldl(fun({TopicName, PartInfo}, IntAcc) ->
        lists:foldl(fun({PartID, ErrCode, Offset}, IntAcc1) ->
            Resp = case kofta_util:error_to_atom(ErrCode) of
                ok ->
                    {ok, Offset};
                Error ->
                    {error, Error}
            end,
            dict:store({TopicName, PartID}, Resp, IntAcc1)
        end, IntAcc, PartInfo)
    end, dict:new(), Body),

    dict:map(fun({Topic, Partition}, Clients) ->
        lists:map(fun(Client) ->
            gen_server:reply(Client, dict:fetch({Topic, Partition}, Responses))
        end, Clients)
    end, ClientDict),

    State#st{clients=dict:new(), msgs=dict:new(), last_batch=now()}.


-spec get_timeout(State) -> Timeout when
    State :: #st{},
    Timeout :: integer().

get_timeout(State) ->
    #st{
        last_batch=LastBatch,
        max_latency=MaxLatency
    } = State,

    NowDiff = timer:now_diff(os:timestamp(), LastBatch)/1000,
    case NowDiff - MaxLatency of
        Diff when Diff < 0 ->
            0;
        Diff ->
            Diff/1000
    end.


-spec name(Host, Port) -> Name when
    Host :: binary(),
    Port :: integer(),
    Name :: atom().

name(Host, Port) ->
    LHost = binary_to_list(Host),
    LPort = integer_to_list(Port),
    list_to_atom("kofta_producer_batcher_" ++ LHost ++ "_" ++ LPort).
