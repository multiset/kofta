-module(kofta_cluster_sup).

-behaviour(supervisor).

-export([
    start_link/0,
    init/1
]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, Brokers} = application:get_env(kofta, brokers),
    Children = lists:map(fun({Host, Port}) ->
        Name = kofta_broker_sup:name(Host, Port),
        {
            Name,
            {kofta_broker_sup, start_link, [Host, Port]},
            permanent, 5000, supervisor, [kofta_broker_sup]
        }
    end, Brokers),
    {ok, {{one_for_one, 5, 10}, Children}}.
