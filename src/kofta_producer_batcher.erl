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
    max_latency,
    host,
    port
}).

-include("kofta.hrl").


-spec send(TopicName, Partition, [{Key, Value}]) -> {ok, Offset} | Error when
    TopicName :: binary(),
    Partition :: integer(),
    Key :: binary() | null,
    Value :: binary() | null,
    Offset :: integer(),
    Error :: {error, any()}.

send(Topic, Partition, KVs) ->
    try kofta_metadata:get_leader(Topic, Partition) of
        {ok, {Host, Port}} ->
            gen_server:call(
                name(Host, Port),
                {msg, Topic, Partition, KVs},
                call_timeout()
            );
        {error, Reason} ->
            {error, Reason}
    catch exit:Reason ->
        {error, Reason}
    end.


start_link(Host, Port) ->
    gen_server:start_link({local, name(Host, Port)}, ?MODULE, [Host, Port], []).


init([Host, Port]) ->
    {ok, BatchLatency} = application:get_env(kofta, batch_latency),
    State = #st{
        clients=dict:new(),
        last_batch=now(),
        msgs=dict:new(),
        max_latency=BatchLatency,
        host=Host,
        port=Port
    },
    {ok, State}.


handle_call({msg, Topic, Partition, KVs}, From, State0) ->
    #st{clients=Clients, msgs=Msgs} = State0,
    ?INCREMENT_COUNTER([kofta, producer, messages, received], length(KVs)),
    State1 = State0#st{
        clients=dict:append({Topic, Partition}, From, Clients),
        msgs=dict:append_list({Topic, Partition}, KVs, Msgs)
    },
    format_return(State1).


handle_cast(_Msg, State) ->
    format_return(State).


handle_info(timeout, State) ->
    format_return(State).


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


-spec format_return(State) -> {noreply, State} | {noreply, State, Timeout} when
    State :: #st{},
    Timeout :: integer().

format_return(State) ->
    #st{
        clients=Clients,
        last_batch=LastBatch,
        max_latency=MaxLatency
    } = State,
    HasWaiters = dict:size(Clients) > 0,
    Latency = round(timer:now_diff(os:timestamp(), LastBatch) / 1000),
    % is_empty isn't in r16
    case HasWaiters of
        false ->
            {noreply, State};
        true ->
            case Latency >= MaxLatency of
                true ->
                    case make_request(State) of
                        {ok, NewState} ->
                            ?INCREMENT_COUNTER(
                                [kofta, producer, requests, success]
                            ),
                            {noreply, NewState, MaxLatency};
                        {error, Reason, NewState} ->
                            ?INCREMENT_COUNTER(
                                [kofta, producer, requests, failure]
                            ),
                            {stop, Reason, NewState}
                    end;
                false ->
                    {noreply, State, MaxLatency - Latency}
            end
    end.

-spec make_request(State) -> {ok, State} | {error, Reason, State} when
    State :: #st{},
    Reason :: any().

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
            ?INCREMENT_COUNTER(
                [kofta, producer, messages, transmitted],
                length(Msgs)
            ),
            MsgSet = kofta_encode:message_set(Msgs),
            [<<PartID:32/big-signed-integer,
             (iolist_size(MsgSet)):32/big-signed-integer>>,
             MsgSet]
        end, Partitions),
        [kofta_encode:string(Topic), PartBin]
    end, RequestData),
    Header = <<1:16/big-signed-integer, 10000:32/big-signed-integer>>,
    BinRequest = kofta_encode:request(0, 0, 0, <<>>, [Header,ProduceBinBody]),
    {ok, RequestTimeout} = application:get_env(kofta, request_timeout),
    case kofta_connection:request(Host, Port, BinRequest, RequestTimeout) of
        {error, Error} ->
            %% Die! Otherwise clients won't be able to guarantee exactly-once
            %% delivery.
            {error, Error, State};
        {ok, Response} ->
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
                    gen_server:reply(
                        Client,
                        dict:fetch({Topic, Partition},
                        Responses
                    ))
                end, Clients)
            end, ClientDict),

            NewState = State#st{
                clients=dict:new(),
                msgs=dict:new(),
                last_batch=now()
            },
            {ok, NewState}
    end.


-spec name(Host, Port) -> Name when
    Host :: binary(),
    Port :: integer(),
    Name :: atom().

name(Host, Port) ->
    LHost = binary_to_list(Host),
    LPort = integer_to_list(Port),
    list_to_atom("kofta_producer_batcher_" ++ LHost ++ "_" ++ LPort).

call_timeout() ->
    %% Need to ~guarantee that gen_server:call requests to the
    %% producer_batcher don't time out before internal requests do.
    {ok, ReqTimeout} = application:get_env(kofta, request_timeout),
    {ok, BatchLatency} = application:get_env(kofta, batch_latency),
    ReqTimeout + BatchLatency * 2.
