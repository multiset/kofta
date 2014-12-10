-module(kofta_cluster).

-export([
    get_brokers/0
]).

-spec get_brokers() -> [{binary(), integer()}].
get_brokers() ->
    {ok, Brokers} = application:get_env(kofta, brokers),
    [HP || {_, HP} <- lists:sort([{random:uniform(), HP} || HP <- Brokers])].
