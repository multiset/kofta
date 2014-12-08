-module(kofta_connection_pool_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Spec = {
        kofta_connection_sup, {kofta_connection_sup, start_link, []},
        transient, 5000, supervisor, [kofta_connection_sup]
    },
    {ok, {{simple_one_for_one, 5, 10}, [Spec]}}.
