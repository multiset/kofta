-module(kofta_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    ets:new(kofta_metadata, [named_table, public]),
    ets:new(broker_id_pids, [named_table, public]),
    ets:new(broker_host_pids, [named_table, public]),
    {ok, {{one_for_one, 5, 10}, [
        ?CHILD(kofta_connection_pool_sup, supervisor),
        ?CHILD(kofta_producer_batcher_sup, supervisor),
        ?CHILD(kofta_metadata_batcher, worker)
    ]}}.
