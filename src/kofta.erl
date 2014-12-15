-module(kofta).

-export([
    metadata/1,
    produce/3,
    fetch/3
]).

-export([start/0, stop/0]).

start() ->
    application:start(kofta).

stop() ->
    application:stop(kofta).

-spec metadata(binary()) -> {ok, [any()]} | {error, any()}.
metadata(TopicName) ->
    kofta_metadata:lookup(TopicName).

-spec produce(binary(), [{binary(), binary()}], [any()]) -> ok | {error, any()}.
produce(Topic, KVs, Options) ->
    {partition, Partition} = lists:keyfind(partition, 1, Options),
    kofta_producer_batcher:send(Topic, Partition, KVs).

-spec fetch(binary(), integer(), [any()]) -> {ok, [binary()]} | {error, any()}.
fetch(Topic, PartID, Options) ->
    {ok, {Host, Port}} = kofta_metadata:get_leader(Topic, PartID),
    Offset = proplists:get_value(offset, Options, 0),
    MaxBytes = proplists:get_value(max_bytes, Options, 10000),
    Message = kofta_fetch:encode([{Topic, [{PartID, Offset, MaxBytes}]}]),
    {ok, Response} = kofta_connection:request(Host, Port, Message),
    kofta_fetch:decode(Response).
