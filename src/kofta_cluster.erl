-module(kofta_cluster).

-export([
    get_broker/2,
    get_random_broker/0
]).

get_broker(Host, Port) ->
    case ets:lookup(broker_host_pids, {Host, Port}) of
        [] ->
            {error, not_found};
        [{_,Pid}] ->
            {ok, Pid}
    end.

-spec get_random_broker() -> pid().
get_random_broker() ->
    Brokers = ets:tab2list(broker_host_pids),
    Len = length(Brokers),
    {_BrokerID, Pid} = lists:nth(random:uniform(Len), Brokers),
    Pid.
