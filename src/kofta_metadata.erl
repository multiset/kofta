-module(kofta_metadata).

-behaviour(gen_fsm).

-export([start_link/2]).

-export([
    init/1,
    ready/2,
    ready/3,
    disconnected/2,
    disconnected/3,
    handle_event/3,
    handle_sync_event/4,
    handle_info/3,
    terminate/3,
    code_change/4
]).

-export([
    lookup/1,
    get_leader/2,
    name/2
]).

-record(st, {
    clients,
    last_batch,
    max_latency=100,
    host,
    port,
    status,
    last_reconnect,
    reconnect_interval=1000
}).

-record(topic, {
    name,
    partitions,
    status
}).

-record(partition, {
    id,
    leader,
    status
}).

get_leader(TopicName, PartitionID) ->
    case ets_lru:lookup_d(kofta_leader_lru, {TopicName, PartitionID}) of
        {ok, Leader} ->
            {ok, Leader};
        not_found ->
            case lookup(TopicName) of
                {ok, Topic} ->
                    Partition = lists:keyfind(
                        PartitionID,
                        #partition.id,
                        Topic#topic.partitions
                    ),
                    {ok, Partition#partition.leader};
                {error, Reason} ->
                    {error, Reason}
            end
    end.

lookup(TopicName) ->
    {ok, {Host, Port}} = kofta_cluster:get_active_broker(),
    case gen_fsm:sync_send_event(name(Host, Port), {lookup, TopicName}) of
        {ok, Topic} ->
            lists:map(fun(#partition{id=ID, leader=Leader}) ->
                ets_lru:insert(kofta_leader_lru, {TopicName, ID}, Leader)
            end, Topic#topic.partitions),
            {ok, Topic};
        {error, Reason} ->
            {error, Reason}
    end.

start_link(Host, Port) ->
    gen_fsm:start_link({local, name(Host, Port)}, ?MODULE, [Host, Port], []).

init([Host, Port]) ->
    State = #st{
        host=Host,
        port=Port,
        last_batch=now(),
        last_reconnect=now(),
        clients=dict:new()
    },
    {ok, disconnected, State, 0}.

do_request(TopicNames, Host, Port) ->
    Data = encode(TopicNames),
    case kofta_connection:request(Host, Port, Data) of
        {ok, Response} ->
            {ok, _Brokers, Topics} = decode(Response),
            {ok, Topics};
        {error, Reason} ->
            {error, Reason}
    end.

reconnect(Host, Port) ->
    case do_request([], Host, Port) of
        {ok, _} ->
            true;
        {error, _Reason} ->
            false
    end.

disconnected(timeout, State) ->
    #st{host=Host, port=Port, reconnect_interval=Timeout} = State,
    case reconnect(Host, Port) of
        true ->
            kofta_cluster:activate_broker(Host, Port),
            {next_state, ready, State, Timeout};
        false ->
            {next_state, disconnected, State, Timeout}
    end.

disconnected(_Msg, _From, State) ->
    #st{
        last_reconnect=LastReconnect,
        reconnect_interval=ReconnectInterval,
        host=Host,
        port=Port
    } = State,

    {NextState, Timeout} = case get_timeout(LastReconnect, ReconnectInterval) of
        0 ->
            case reconnect(Host, Port) of
                true ->
                    {ready, 0};
                false ->
                    {disconnected, ReconnectInterval}
            end;
        Timeout0 ->
            {disconnected, Timeout0}
    end,

    NewState = State#st{last_reconnect=os:timestamp()},
    {reply, {error, broker_down}, NextState, NewState, Timeout}.

ready(timeout, State) ->
    maybe_timeout(State).

ready({lookup, Topic}, From, State0) ->
    #st{clients=Clients} = State0,
    State1 = State0#st{
        clients=dict:append(Topic, From, Clients)
    },
    maybe_timeout(State1).

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
    {reply, ok, StateName, State}.

handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

maybe_timeout(State) ->
    #st{
        clients=Clients,
        last_batch=LastBatch,
        max_latency=MaxLatency
    } = State,
    % is_empty isn't in r16
    case dict:size(Clients) of
        0 ->
            {next_state, ready, State};
        _ ->
            case get_timeout(LastBatch, MaxLatency) of
                0 ->
                    make_request(State);
                Timeout ->
                    {next_state, ready, State, Timeout}
            end
    end.

make_request(State) ->
    #st{
        clients=ClientDict,
        host=Host,
        port=Port,
        reconnect_interval=Timeout
    } = State,

    TopicNames = dict:fetch_keys(ClientDict),

    NewState = State#st{clients=dict:new(), last_batch=now()},

    case do_request(TopicNames, Host, Port) of
        {ok, Topics} ->
            lists:map(fun(Topic) ->
                Clients = dict:fetch(Topic#topic.name, ClientDict),
                lists:map(fun(Client) ->
                    gen_server:reply(Client, {ok, Topic})
                end, Clients)
            end, Topics),
            {next_state, ready, NewState};
        {error, Reason} ->
            kofta_cluster:deactivate_broker(Host, Port),
            lists:map(fun(TopicName) ->
                Clients = dict:fetch(TopicName, ClientDict),
                lists:map(fun(Client) ->
                    gen_server:reply(Client, {error, Reason})
                end, Clients)
            end, TopicNames),

            {next_state, disconnected, NewState, Timeout}
    end.


get_timeout(Last, Max) ->
    NowDiff = timer:now_diff(os:timestamp(), Last)/1000,
    Timeout = Max - NowDiff,
    max(0, round(Timeout)).


-spec encode([binary()]) -> binary().
encode(Topics) ->
    Message = kofta_encode:array(fun kofta_encode:string/1, Topics),
    kofta_encode:request(3, 0, 0, <<"">>, Message).

decode(Binary) ->
    {_Request, Rest0} = kofta_decode:request(Binary),
    {Brokers, Rest1} = kofta_decode:array(fun kofta_decode:broker/1, Rest0),
    {TopicMetadata, <<>>} = kofta_decode:array(
        fun kofta_decode:topic_metadata/1, Rest1),

    BrokerHosts = lists:foldl(fun({BrokerID, Host, Port}, Acc) ->
        dict:store(BrokerID, {Host, Port}, Acc)
    end, dict:new(), Brokers),

    Topics = lists:map(fun({TopicErr, Name, PartInfo}) ->
        TopicStatus = kofta_util:error_to_atom(TopicErr),
        Topic = #topic{name=Name, status=TopicStatus},
        Parts = lists:map(fun({PartErr, PartID, Leader, _Reps, _Isr}) ->
            PartStatus = kofta_util:error_to_atom(PartErr),
            LeaderHP = dict:fetch(Leader, BrokerHosts),
            #partition{id=PartID, leader=LeaderHP, status=PartStatus}
        end, PartInfo),
        Topic#topic{partitions=Parts}
    end, TopicMetadata),
    {ok, Brokers, Topics}.

name(Host, Port) ->
    list_to_atom("kofta_metadata_" ++ binary_to_list(Host) ++ "_" ++ integer_to_list(Port)).
