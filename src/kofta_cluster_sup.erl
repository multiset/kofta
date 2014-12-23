-module(kofta_cluster_sup).

-behaviour(supervisor).

-export([
    start_link/0,
    init/1
]).

-export([
    start_broker/2,
    stop_broker/2
]).


start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


init([]) ->
    ets:new(active_brokers, [named_table, public]),
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


-spec start_broker(Host, Port) -> supervisor:startchild_ret() when
    Host :: binary(),
    Port :: integer().

start_broker(Host, Port) ->
    Spec = {
        kofta_broker_sup:name(Host, Port),
        {kofta_broker_sup, start_link, [Host, Port]},
        permanent, 5000, supervisor, [kofta_broker_sup]
    },
    supervisor:start_child(?MODULE, Spec).


-spec stop_broker(Host, Port) -> ok | {error, Error} when
    Host :: binary(),
    Port :: integer(),
    Error :: not_found | simple_one_for_one.

stop_broker(Host, Port) ->
    Name = kofta_broker_sup:name(Host, Port),
    supervisor:terminate_child(?MODULE, Name).
