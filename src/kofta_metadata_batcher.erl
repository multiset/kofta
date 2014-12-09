-module(kofta_metadata_batcher).

-behaviour(gen_server).

-export([refresh/1]).

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

refresh(Topic) ->
    gen_server:call(?MODULE, {refresh, Topic}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    {ok, Hosts} = application:get_env(kofta, brokers),
    lists:map(fun({Host, Port}) ->
        supervisor:start_child(kofta_broker_sup, [Host, Port])
    end, Hosts),
    State = #state{
        clients=dict:new(),
        last_batch=now()
    },
    {ok, State}.

handle_call({refresh, Topic}, From, State0) ->
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

    Topics = dict:fetch_keys(ClientDict),
    Data = encode(Topics),

    Broker = kofta_cluster:get_random_broker(),
    {ok, Response} = kofta_broker:request(Broker, Data),
    Decoded = decode(Response),

    dict:map(fun(_Topic, Clients) ->
        lists:map(fun(Client) ->
            gen_server:reply(Client, {ok, Decoded})
        end, Clients)
    end, ClientDict),

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


encode(Topics) ->
    Message = kofta_encode:array(fun kofta_encode:string/1, Topics),
    kofta_encode:request(3, 0, 0, <<"">>, Message).

decode(Binary) ->
    {Request, Rest0} = kofta_decode:request(Binary),
    {Brokers, Rest1} = kofta_decode:array(fun kofta_decode:broker/1, Rest0),
    {TopicMetadata, <<>>} = kofta_decode:array(
        fun kofta_decode:topic_metadata/1, Rest1),
    {Request, Brokers, TopicMetadata}.
