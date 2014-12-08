-module(kofta).

-export([
    metadata/2,
    produce/4,
    fetch/3
]).

-export([start/0, stop/0]).

start() ->
    % TODO: Fix this mess
    S = application:start(kofta),
    {ok, Hosts} = application:get_env(kofta, brokers),
    lists:map(fun({Host, Port}) ->
        supervisor:start_child(kofta_broker_sup, [Host, Port])
    end, Hosts),
    S.

stop() ->
    application:stop(kofta).

-spec metadata(binary(), [any()]) -> {ok, [any()]} | {error, any()}.
metadata(Topic, Options) ->
    case lists:member(cached, Options) of
        true ->
            case ets:lookup(kofta_metadata, Topic) of
                [] ->
                    {error, not_found};
                [{_Topic, Result}] ->
                    {ok, Result}
            end;
        false ->
            case kofta_metadata_batcher:refresh(Topic) of
                {ok, Result} ->
                    ets:insert(kofta_metadata, {Topic, Result}),
                    {ok, Result};
                {error, Reason} ->
                    {error, Reason}
            end
    end.

-spec produce(binary(), binary(), binary(), [any()]) -> ok.
produce(Topic, Key, Value, Options) ->
    {partition, Partition} = lists:keyfind(partition, 1, Options),
    kofta_producer_batcher:send(Topic, Partition, Key, Value).

-spec fetch(binary(), integer(), [any()]) -> {ok, [binary()]} | {error, any()}.
fetch(Topic, PartID, Options) ->
    {ok, Metadata} = case metadata(Topic, [cached]) of
        {error, not_found} ->
            metadata(Topic, []);
        Other ->
            Other
    end,
    Offset = proplists:get_value(offset, Options, 0),
    MaxBytes = proplists:get_value(max_bytes, Options, 10000),
    {_, Brokers, TopicMetadata} = Metadata,
    {_Error, _Topic, Parts} = lists:keyfind(Topic, 2, TopicMetadata),
    {_PError, _Part, Leader, _Replicas, _Isr} = lists:keyfind(PartID, 2, Parts),
    {_, LeaderHost, LeaderPort} = lists:keyfind(Leader, 1, Brokers),
    case kofta_cluster:get_broker(LeaderHost, LeaderPort) of
        {ok, Broker} ->
            Message = kofta_fetch:encode([{Topic, [{PartID, Offset, MaxBytes}]}]),
            {ok, Response} = kofta_broker:request(Broker, Message),
            kofta_fetch:decode(Response);
        {error, not_found} ->
            leader_not_found
    end.
