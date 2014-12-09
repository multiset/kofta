-module(kofta_metadata_batcher).

-behaviour(gen_server).

-export([lookup/2]).

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
    last_batch,
    max_latency=100
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

lookup(TopicName, Options) ->
    case lists:member(cached, Options) of
        true ->
            case ets:lookup(kofta_metadata, TopicName) of
                [] ->
                    {error, not_found};
                [Topic] ->
                    {ok, Topic}
            end;
        false ->
            case gen_server:call(?MODULE, {lookup, TopicName}) of
                {ok, Topic} ->
                    ets:insert(kofta_metadata, Topic),
                    {ok, Topic};
                {error, Reason} ->
                    {error, Reason}
        end
    end.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    State = #state{
        clients=dict:new(),
        last_batch=now()
    },
    {ok, State}.

handle_call({lookup, Topic}, From, State0) ->
    #state{clients=Clients} = State0,
    State1 = State0#state{
        clients=dict:append(Topic, From, Clients)
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
        clients=ClientDict
    } = State,

    TopicNames = dict:fetch_keys(ClientDict),
    Data = encode(TopicNames),
    Brokers = kofta_cluster:get_brokers(),

    Success = lists:foldl(fun(Broker, Acc) ->
        case Acc of
            false ->
                case kofta_broker:request(Broker, Data) of
                    {ok, Response} ->
                        {ok, Topics} = decode(Response),
                        lists:map(fun(Topic) ->
                            Clients = dict:fetch(Topic#topic.name, ClientDict),
                            lists:map(fun(Client) ->
                                gen_server:reply(Client, {ok, Topic})
                            end, Clients)
                        end, Topics),
                        true;
                    {error, _Reason} ->
                        false
                end;
            true ->
                true
        end
    end, false, Brokers),

    case Success of
        true ->
            ok;
        false ->
            dict:map(fun(_Topic, Clients) ->
                lists:map(fun(Client) ->
                    gen_server:reply(Client, {error, all_brokers_down})
                end, Clients)
            end, ClientDict)
    end,

    State#state{clients=dict:new(), last_batch=now()}.

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

    Result = lists:map(fun({TopicErr, Name, PartInfo}) ->
        TopicStatus = kofta_util:error_to_atom(TopicErr),
        Topic = #topic{name=Name, status=TopicStatus},
        Parts = lists:map(fun({PartErr, PartID, Leader, _Reps, _Isr}) ->
            PartStatus = kofta_util:error_to_atom(PartErr),
            LeaderHP = dict:fetch(Leader, BrokerHosts),
            #partition{id=PartID, leader=LeaderHP, status=PartStatus}
        end, PartInfo),
        Topic#topic{partitions=Parts}
    end, TopicMetadata),
    {ok, Result}.
