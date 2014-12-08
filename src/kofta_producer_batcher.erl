-module(kofta_producer_batcher).

-behaviour(gen_server).

-export([send/4]).

-export([start_link/0]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(state, {
    clients,
    msgs,
    last_batch,
    max_latency=100
}).

send(Topic, Partition, Key, Value) ->
    gen_server:call(?MODULE, {msg, Topic, Partition, Key, Value}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    State = #state{
        clients=dict:new(),
        last_batch=now(),
        msgs=dict:new()
    },
    {ok, State}.

handle_call({msg, Topic, Partition, Key, Value}, From, State0) ->
    #state{clients=Clients, msgs=Msgs} = State0,
    State1 = State0#state{
        clients=dict:append({Topic, Partition}, From, Clients),
        msgs=dict:append({Topic, Partition}, {Key, Value}, Msgs)
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

format_return(Type, State) ->
    #state{
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

maybe_make_request(State) ->
    #state{last_batch=LastBatch, max_latency=MaxLatency} = State,
    case timer:now_diff(os:timestamp(), LastBatch)/1000 of
        Diff when Diff >= MaxLatency ->
            make_request(State);
        _ ->
            State
    end.

make_request(State) ->
    #state{
        clients=ClientDict,
        msgs=MsgDict
    } = State,

    LeaderMsgs = dict:fold(fun({Topic, Partition}, KVs, Acc) ->
        {ok, {_, Brokers, TInfo}} = case kofta:metadata(Topic, [cached]) of
            {error, not_found} ->
                kofta:metadata(Topic, []);
            {ok, Metadata} ->
                {ok, Metadata}
        end,
        {_TErr, _Topic, PInfo} = lists:keyfind(Topic, 2, TInfo),
        {_PErr, _PartID, Leader, _Rep, _Isr} = lists:keyfind(Partition, 2, PInfo),
        {_Leader, Host, Port} = lists:keyfind(Leader, 1, Brokers),
        dict:append({Host, Port}, {Topic, Partition, KVs}, Acc)
    end, dict:new(), MsgDict),

    Aggregated = dict:map(fun(_HostPort, PartMsgs) ->
        {FinalTopic, FinalTAcc, FinalAcc} = lists:foldl(fun
            ({Topic, PartID, KVs}, {first, [], []}) ->
                {Topic, [{PartID, KVs}], []};
            ({Topic, PartID, KVs}, {Topic, TAcc, Acc}) ->
                {Topic, [{PartID, KVs}|TAcc], Acc};
            ({NewTopic, PartID, KVs}, {OldTopic, TAcc, Acc}) ->
                {NewTopic, [{PartID, KVs}], [{OldTopic, TAcc}|Acc]}
        end, {first, [], []}, lists:keysort(1, PartMsgs)),
        [{FinalTopic, FinalTAcc}|FinalAcc]
    end, LeaderMsgs),

    Requests = dict:map(fun(_HostPort, ProduceData) ->
        ProduceBinBody = kofta_encode:array(fun({Topic, Partitions}) ->
            PartBin = kofta_encode:array(fun({PartID, Msgs}) ->
                MsgSet = kofta_encode:message_set(Msgs),
                [<<PartID:32/big-signed-integer,
                 (iolist_size(MsgSet)):32/big-signed-integer>>,
                 MsgSet]
            end, Partitions),
            [kofta_encode:string(Topic), PartBin]
        end, ProduceData),
        Request = [
            <<1:16/big-signed-integer, 10000:32/big-signed-integer>>,
            ProduceBinBody
        ],
        kofta_encode:request(0, 0, 0, <<>>, Request)
    end, Aggregated),

    % TODO: Parallelize this
    Responses = dict:fold(fun({Host, Port}, Data, Acc) ->
        {ok, Broker} = kofta_cluster:get_broker(Host, Port),
        {ok, Response} = kofta_broker:request(Broker, Data),
        {Request, Rest0} = kofta_decode:request(Response),
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
        lists:foldl(fun({TopicName, PartInfo}, IntAcc) ->
            lists:foldl(fun({PartID, _ErrCode, _Offset}, IntAcc1) ->
                dict:store({TopicName, PartID}, {Request, Body}, IntAcc1)
            end, IntAcc, PartInfo)
        end, Acc, Body)
    end, dict:new(), Requests),

    dict:map(fun({Topic, Partition}, Clients) ->
        lists:map(fun(Client) ->
            gen_server:reply(Client, dict:fetch({Topic, Partition}, Responses))
        end, Clients)
    end, ClientDict),

    State#state{clients=dict:new(), msgs=dict:new(), last_batch=now()}.

get_timeout(State) ->
    #state{
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
