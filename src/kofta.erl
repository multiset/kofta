-module(kofta).

-export([
    metadata/1,
    produce/3,
    fetch/3
]).

-export([
    start/0,
    stop/0
]).


start() ->
    application:start(poolboy),
    application:start(kofta).


stop() ->
    application:stop(poolboy),
    application:stop(kofta).


-spec metadata(TopicName) -> {ok, [PartError | Result]} | Error when
    TopicName :: binary(),
    PartError :: {error, any()},
    Result :: {ok, integer(), {binary(), integer()}},
    Error :: {error, any()}.

metadata(TopicName) ->
    kofta_metadata:lookup(TopicName).


-spec produce(TopicName, KVs, Options) -> ok | Error when
    TopicName :: binary(),
    KVs :: [{binary(), binary()}],
    Options :: [any()],
    Error :: {error, any()}.

produce(Topic, KVs, Options) ->
    {partition, Partition} = lists:keyfind(partition, 1, Options),
    kofta_producer_batcher:send(Topic, Partition, KVs).


-spec fetch(TopicName, PartitionID, Options) -> {ok, Response} | Error when
    TopicName :: binary(),
    PartitionID :: integer(),
    Options :: [any()],
    Response :: [binary()],
    Error :: {error, any()}.

fetch(Topic, PartID, Options) ->
    {ok, {Host, Port}} = kofta_metadata:get_leader(Topic, PartID),
    Offset = proplists:get_value(offset, Options, 0),
    MaxBytes = proplists:get_value(max_bytes, Options, 10000),
    Message = kofta_fetch:encode([{Topic, [{PartID, Offset, MaxBytes}]}]),
    {ok, Response} = kofta_connection:request(Host, Port, Message),
    kofta_fetch:decode(Response).
