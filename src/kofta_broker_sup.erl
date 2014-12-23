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
    {ok, {{one_for_one, 5, 10}, [
        poolboy:child_spec(PoolName, PoolArgs, [Host, Port]),
        {
            kofta_producer_batcher:name(Host, Port),
            {kofta_metadata, start_link, [Host, Port]},
            transient, 5000, worker, [kofta_metadata]
        },
        {
            kofta_metadata:name(Host, Port),
            {kofta_producer_batcher, start_link, [Host, Port]},
            transient, 5000, worker, [kofta_producer_batcher]
        }
    ]}}.


-spec name(Host, Port) -> Name when
    Host :: binary(),
    Port :: integer(),
    Name :: atom().

name(Host, Port) ->
    LHost = binary_to_list(Host),
    LPort = integer_to_list(Port),
    list_to_atom("kofta_broker_sup_" ++ LHost ++ "_" ++ LPort).
