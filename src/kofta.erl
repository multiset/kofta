-module(kofta).

-export([
    metadata/2,
    produce/4,
    fetch/3
]).

-export([start/0, stop/0]).

start() ->
    application:start(kofta).

stop() ->
    application:stop(kofta).

-spec metadata(binary(), [any()]) -> {ok, [any()]} | {error, any()}.
metadata(TopicName, Options) ->
    kofta_metadata_batcher:lookup(TopicName, Options).

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
    Message = kofta_fetch:encode([{Topic, [{PartID, Offset, MaxBytes}]}]),
    {ok, Response} = kofta_connection:request(LeaderHost, LeaderPort, Message),
    kofta_fetch:decode(Response).
