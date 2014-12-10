-module(kofta_connection_pool_sup).

-behaviour(supervisor).

-export([
    start_link/0,
    init/1
]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, Brokers} = application:get_env(kofta, brokers),
    {ok, Size} = application:get_env(kofta, connection_pool_size),
    PoolSpecs = lists:map(fun({Host, Port}) ->
        PoolName = kofta_connection_pool:name(Host, Port),
        PoolArgs = [
            {name, {local, PoolName}},
            {worker_module, kofta_connection},
            {size, Size}
        ],
        poolboy:child_spec(PoolName, PoolArgs, [Host, Port])
    end, Brokers),
    {ok, {{one_for_all, 5, 10}, PoolSpecs}}.
