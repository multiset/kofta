-module(kofta_cluster).

-export([
    activate_broker/2,
    deactivate_broker/2,
    active_broker/0
]).


-spec activate_broker(Host, Port) -> ok when
    Host :: binary(),
    Port :: integer().

activate_broker(Host, Port) ->
    true = ets:insert(active_brokers, {{Host, Port}}),
    ok.


-spec deactivate_broker(Host, Port) -> ok when
    Host :: binary(),
    Port :: integer().

deactivate_broker(Host, Port) ->
    true = ets:delete(active_brokers, {Host, Port}),
    ok.


-spec active_broker() -> Error | {ok, Broker} when
    Error :: {error, any()},
    Broker :: {binary(), integer()}. % host, port

active_broker() ->
    Tab = ets:tab2list(active_brokers),
    Size = length(Tab),
    case Size of
        0 ->
            {error, all_brokers_down};
        _ ->
            RandomIndex = element(1, random:uniform_s(Size, os:timestamp())),
            {Broker} = lists:nth(RandomIndex, Tab),
            {ok, Broker}
    end.
