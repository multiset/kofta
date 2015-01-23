-module(kofta_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(CHILD(I, Type, A), {I, {I, start_link, A}, permanent, 5000, Type, [I]}).


start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


init([]) ->
    folsom_metrics:new_histogram(
        [kofta, requests, checkout_latency],
        slide_uniform,
        {10, 1024}
    ),
    folsom_metrics:new_histogram(
        [kofta, requests, latency],
        slide_uniform,
        {10, 1024}
    ),
    folsom_metrics:new_counter([kofta, requests, success]),
    folsom_metrics:new_counter([kofta, requests, timeout]),
    folsom_metrics:new_counter([kofta, requests, error]),
    folsom_metrics:new_counter([kofta, connections, inits, success]),
    folsom_metrics:new_counter([kofta, connections, inits, failure]),
    folsom_metrics:new_counter([kofta, cluster, broker_activations]),
    folsom_metrics:new_counter([kofta, cluster, broker_deactivations]),
    folsom_metrics:new_counter([kofta, cluster, all_brokers_down]),
    folsom_metrics:new_counter([kofta, cluster, broker_lookups]),
    folsom_metrics:new_counter([kofta, metadata, leader_lru_hits]),
    folsom_metrics:new_counter([kofta, metadata, leader_lru_misses]),
    folsom_metrics:new_counter([kofta, metadata, bad_partitions]),
    folsom_metrics:new_counter([kofta, metadata, lookup_errors]),
    folsom_metrics:new_counter([kofta, metadata, reconnects, success]),
    folsom_metrics:new_counter([kofta, metadata, reconnects, failure]),
    folsom_metrics:new_counter([kofta, metadata, requests, success]),
    folsom_metrics:new_counter([kofta, metadata, requests, failure]),
    folsom_metrics:new_counter([kofta, producer, requests, success]),
    folsom_metrics:new_counter([kofta, producer, requests, failure]),
    folsom_metrics:new_counter([kofta, producer, messages, received]),
    folsom_metrics:new_counter([kofta, producer, messages, transmitted]),
    {ok, {{one_for_one, 5, 10}, [
        ?CHILD(ets_lru, worker, [kofta_leader_lru, [{max_size, 1024*1024}]]),
        ?CHILD(kofta_cluster_sup, supervisor, [])
    ]}}.
