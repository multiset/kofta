-module(kofta_decode).

-export([
    string/1,
    bytes/1,
    array/2,
    request/1,
    broker/1,
    topic_metadata/1,
    partition_metadata/1,
    message_set/2
]).


-spec string(EncodedString) -> {DecodedString, Rest} when
    EncodedString :: binary(),
    DecodedString :: binary(),
    Rest :: binary().

string(Binary) ->
    <<Length:16/big-signed-integer, Rest0/binary>> = Binary,
    <<String:Length/binary, Rest1/binary>> = Rest0,
    {String, Rest1}.

-spec bytes(EncodedBytes) -> {DecodedBytes, Rest} | undefined when
    EncodedBytes :: binary(),
    DecodedBytes :: binary() | null,
    Rest :: binary().

bytes(Binary) when bit_size(Binary) < 32 ->
    undefined;
bytes(Binary) ->
    <<Length:32/big-signed-integer, Rest0/binary>> = Binary,
    case Length of
        -1 ->
            {null, Rest0};
        _ when bit_size(Rest0) < Length * 8 ->
            undefined;
        _ ->
            <<Bytes:Length/binary, Rest1/binary>> = Rest0,
            {Bytes, Rest1}
    end.

-spec array(DecoderFun, ArrayBinary) -> {DecodedArray, Rest} when
    DecoderFun :: fun(),
    ArrayBinary :: binary(),
    DecodedArray :: [any()],
    Rest :: binary().

array(DecodeElement, Binary) ->
    <<Length:32/big-signed-integer, Rest/binary>> = Binary,
    array_int(DecodeElement, Rest, Length, []).

array_int(_DecodeElement, Binary, 0, Acc) ->
    {lists:reverse(Acc), Binary};
array_int(DecodeElement, Binary, Count, Acc) ->
    {Element, Rest} = DecodeElement(Binary),
    array_int(DecodeElement, Rest, Count-1, [Element|Acc]).


-spec request(EncodedRequest) -> {{RequestSize, CorrelationID}, Rest} when
    EncodedRequest :: binary(),
    RequestSize :: integer(),
    CorrelationID :: integer(),
    Rest :: binary().

request(Message) ->
    <<Size:32/big-signed-integer,
      CorrelationID:32/big-signed-integer,
      Rest0/binary>> = Message,
    {{Size, CorrelationID}, Rest0}.


-spec broker(EncodedBroker) -> {{NodeID, Host, Port}, Rest} when
    EncodedBroker :: binary(),
    NodeID :: integer(),
    Host :: binary(),
    Port :: integer(),
    Rest :: binary().

broker(Binary) ->
    <<NodeID:32/big-signed-integer, Rest/binary>> = Binary,
    {Host, Rest1} = string(Rest),
    <<Port:32/big-signed-integer, Rest2/binary>> = Rest1,
    {{NodeID, Host, Port}, Rest2}.


-spec topic_metadata(EncodedMetadata) -> {{Error, Name, PartData}, Rest} when
    EncodedMetadata :: binary(),
    Error :: integer(),
    Name :: binary(),
    PartData :: [{
        integer(),
        integer(),
        {binary(), integer()},
        [integer()],
        [integer()]
    }],
    Rest :: binary().

topic_metadata(Binary) ->
    <<TopicErrorCode:16/big-signed-integer, Rest0/binary>> = Binary,
    {TopicName, Rest1} = string(Rest0),
    {PartitionMetadata, Rest2} = array(fun partition_metadata/1, Rest1),
    {{TopicErrorCode, TopicName, PartitionMetadata}, Rest2}.


-spec int32(EncodedInt32) -> {Int32, Rest} when
    EncodedInt32 :: binary(),
    Int32 :: integer(),
    Rest :: binary().

int32(Binary) ->
    <<Int:32/big-signed-integer, Rest/binary>> = Binary,
    {Int, Rest}.


-spec partition_metadata(EncodedPartData) -> {PartMetadata, Rest} when
    EncodedPartData :: binary(),
    PartMetadata :: {
        integer(),
        integer(),
        {binary(), integer()},
        [integer()],
        [integer()]
    },
    Rest :: binary().

partition_metadata(Binary) ->
    <<PartitionErrorCode:16/big-signed-integer,
      PartitionID:32/big-signed-integer,
      Leader:32/big-signed-integer,
      Rest0/binary>> = Binary,
    {Replicas, Rest1} = array(fun int32/1, Rest0),
    {Isr, Rest2} = array(fun int32/1, Rest1),
    {{PartitionErrorCode, PartitionID, Leader, Replicas, Isr}, Rest2}.


-spec message_set(EncodedMessageSet, MessageSetSize) -> {Messages, Rest} when
    EncodedMessageSet :: binary(),
    MessageSetSize :: integer(),
    Messages :: {
        integer(),
        integer(),
        integer(),
        integer(),
        integer(),
        binary() | null,
        binary() | null
    },
    Rest :: binary().

message_set(Binary, Size) when bit_size(Binary) < (Size * 8) ->
    {undefined, <<>>};
message_set(Binary, Size) ->
    <<MSBin:Size/binary, Rest0/binary>> = Binary,
    {ParsedMS, Rest1} = message_set_int(MSBin, []),
    case Rest1 of
        <<>> ->
            ok;
        _ ->
            %% TODO: log
            ok
    end,
    {ParsedMS, Rest0}.

message_set_int(<<>>=Binary, Acc) ->
    {lists:reverse(Acc), Binary};
message_set_int(Binary, Acc) when bit_size(Binary) < 144 ->
    {lists:reverse(Acc), Binary};
message_set_int(Binary, Acc0) ->
    <<Offset:64/big-signed-integer,
      MessageSize:32/big-signed-integer,
      Crc:32/big-signed-integer,
      MagicByte:8/big-signed-integer,
      Attributes:8/big-signed-integer,
      Rest0/binary>> = Binary,
    case bytes(Rest0) of
        undefined ->
            %% This message is truncated - bail.
            {lists:reverse(Acc0), Binary};
        {Key, Rest1} ->
            case bytes(Rest1) of
                undefined ->
                    %% This message is truncated - bail.
                    {lists:reverse(Acc0), Binary};
                {Value, Rest2} ->
                    Message = {
                        Offset,
                        MessageSize,
                        Crc,
                        MagicByte,
                        Attributes,
                        Key,
                        Value
                    },
                    message_set_int(Rest2, [Message|Acc0])
            end
    end.
