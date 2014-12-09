-module(kofta_cluster).

-export([
    get_broker/2,
    get_brokers/0
]).

get_broker(Host, Port) ->
    case ets:lookup(broker_host_pids, {Host, Port}) of
        [] ->
            {error, not_found};
        [{_,Pid}] ->
            {ok, Pid}
    end.

-spec get_brokers() -> [pid()].
get_brokers() ->
    Brokers = ets:tab2list(broker_host_pids),
    Shuffled = lists:sort([{random:uniform(), Pid} || {_BID, Pid} <- Brokers]),
    [Pid || {_, Pid} <- Shuffled].
