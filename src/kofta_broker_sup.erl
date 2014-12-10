-module(kofta_broker_sup).

-behaviour(supervisor).

-export([
    start_link/2,
    init/1,
    name/2
]).

start_link(Host, Port) ->
    supervisor:start_link({local, name(Host, Port)}, ?MODULE, [Host, Port]).

init([Host, Port]) ->
    {ok, Size} = application:get_env(kofta, connection_pool_size),
    PoolName = kofta_connection:name(Host, Port),
    PoolArgs = [
        {name, {local, PoolName}},
        {worker_module, kofta_connection},
        {size, Size}
    ],
    Name = kofta_producer_batcher:name(Host, Port),
    {ok, {{one_for_one, 5, 10}, [
        poolboy:child_spec(PoolName, PoolArgs, [Host, Port]),
        {
            Name,
            {kofta_producer_batcher, start_link, [Host, Port]},
            permanent, 5000, worker, [kofta_producer_batcher]
        }
    ]}}.

name(Host, Port) ->
    LHost = binary_to_list(Host),
    LPort = integer_to_list(Port),
    list_to_atom("kofta_broker_sup_" ++ LHost ++ "_" ++ LPort).
