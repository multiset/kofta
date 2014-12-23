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


-spec fetch(TopicName, PartitionID, Options) -> {ok, [Message]} | Error when
    TopicName :: binary(),
    PartitionID :: integer(),
    Options :: [any()],
    Message :: [{Offset, Key, Value}],
    Key :: binary() | null,
    Value :: binary() | null,
    Offset :: integer(),
    Error :: {error, any()}.

fetch(Topic, PartID, Options) ->
    case kofta_metadata:get_leader(Topic, PartID) of
        {ok, {Host, Port}} ->
            ReqOffset = proplists:get_value(offset, Options, 0),
            MaxBytes = proplists:get_value(max_bytes, Options, 10000),
            Request = kofta_fetch:encode(
                [{Topic, [{PartID, ReqOffset, MaxBytes}]}]
            ),
            {ok, Response} = kofta_connection:request(Host, Port, Request),
            {Decoded, _Rest} = kofta_fetch:decode(Response),
            {_Header, Body} = Decoded,
            [{_Topic, [{PartID, ErrorCode, _HWOffset, Messages}]}] = Body,
            case kofta_util:error_to_atom(ErrorCode) of
                ok ->
                    Results = lists:map(fun(Msg) ->
                        {Offset, _Size, _CRC, _Magic, _Att, Key, Value} = Msg,
                        {Offset, Key, Value}
                    end, Messages),
                    {ok, Results};
                Error ->
                    {error, Error}
            end;
        {error, Reason} ->
            {error, Reason}
    end.
