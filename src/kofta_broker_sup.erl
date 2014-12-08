-module(kofta_broker_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, {{simple_one_for_one, 5, 10}, [
        {kofta_broker, {kofta_broker, start_link, []},
         transient, 5000, worker, [kofta_broker]}
    ]}}.
