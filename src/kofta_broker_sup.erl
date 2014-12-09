-module(kofta_broker_sup).

-behaviour(supervisor).

-export([
    start_link/0,
    init/1,
    start_broker/2
]).

broker_spec(Host, Port) ->
    ID = "kofta_broker_" ++ binary_to_list(Host) ++ "_" ++ integer_to_list(Port),
    {
        ID,
        {kofta_broker, start_link, [Host, Port]},
        transient, 5000, worker, [kofta_broker]
    }.

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, Hosts} = application:get_env(kofta, brokers),
    Children = lists:map(fun({Host, Port}) ->
        broker_spec(Host, Port)
    end, Hosts),
    {ok, {{one_for_one, 5, 10}, Children}}.

start_broker(Host, Port) ->
    supervisor:start_child(?MODULE, broker_spec(Host, Port)).
