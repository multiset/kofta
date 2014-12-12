-module(kofta_cluster).

-export([
    activate_broker/2,
    deactivate_broker/2,
    get_active_broker/0,
    get_brokers/0
]).

activate_broker(Host, Port) ->
    ets:insert(active_brokers, {{Host, Port}}).

deactivate_broker(Host, Port) ->
    ets:delete(active_brokers, {Host, Port}).

get_active_broker() ->
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

-spec get_brokers() -> [{binary(), integer()}].
get_brokers() ->
    {ok, Brokers} = application:get_env(kofta, brokers),
    [HP || {_, HP} <- lists:sort([{random:uniform(), HP} || HP <- Brokers])].
