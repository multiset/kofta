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

-spec string(binary()) -> {binary(), binary()}.
string(Binary) ->
    <<Length:16/big-signed-integer, Rest0/binary>> = Binary,
    <<String:Length/binary, Rest1/binary>> = Rest0,
    {String, Rest1}.

-spec bytes(binary()) -> binary().
bytes(Binary) ->
    <<Length:32/big-signed-integer, Rest0/binary>> = Binary,
    <<Bytes:Length/binary, Rest1/binary>> = Rest0,
    {Bytes, Rest1}.

-spec array(fun(), binary()) -> {any(), binary()}.
array(DecodeElement, Binary) ->
    <<Length:32/big-signed-integer, Rest/binary>> = Binary,
    array_int(DecodeElement, Rest, Length, []).

array_int(_DecodeElement, Binary, 0, Acc) ->
    {lists:reverse(Acc), Binary};
array_int(DecodeElement, Binary, Count, Acc) ->
    {Element, Rest} = DecodeElement(Binary),
    array_int(DecodeElement, Rest, Count-1, [Element|Acc]).

request(Message) ->
    <<Size:32/big-signed-integer,
      CorrelationID:32/big-signed-integer,
      Rest0/binary>> = Message,
    {{Size, CorrelationID}, Rest0}.

broker(Binary) ->
    <<NodeID:32/big-signed-integer, Rest/binary>> = Binary,
    {Host, Rest1} = string(Rest),
    <<Port:32/big-signed-integer, Rest2/binary>> = Rest1,
    {{NodeID, Host, Port}, Rest2}.

topic_metadata(Binary) ->
    <<TopicErrorCode:16/big-signed-integer, Rest0/binary>> = Binary,
    {TopicName, Rest1} = string(Rest0),
    {PartitionMetadata, Rest2} = array(fun partition_metadata/1, Rest1),
    {{TopicErrorCode, TopicName, PartitionMetadata}, Rest2}.

int32(Binary) ->
    <<Int:32/big-signed-integer, Rest/binary>> = Binary,
    {Int, Rest}.

partition_metadata(Binary) ->
    <<PartitionErrorCode:16/big-signed-integer,
      PartitionID:32/big-signed-integer,
      Leader:32/big-signed-integer,
      Rest0/binary>> = Binary,
    {Replicas, Rest1} = array(fun int32/1, Rest0),
    {Isr, Rest2} = array(fun int32/1, Rest1),
    {{PartitionErrorCode, PartitionID, Leader, Replicas, Isr}, Rest2}.

message_set(Binary, Size) ->
    message_set_int(Binary, Size, []).

message_set_int(Binary, 0, Acc) ->
    {lists:reverse(Acc), Binary};
message_set_int(Binary, RestCount, Acc0) ->
    <<Offset:64/big-signed-integer,
      MessageSize:32/big-signed-integer,
      Crc:32/big-signed-integer,
      MagicByte:8/big-signed-integer,
      Attributes:8/big-signed-integer,
      KeyLen:32/big-signed-integer,
      Rest0/binary>> = Binary,

    {Key, SizeCount0, ValueLen, Rest2} = case KeyLen of
        -1 ->
            <<ValueLen0:32/big-signed-integer,
              Rest1/binary>> = Rest0,
            % Obviously 26
            {null, 26, ValueLen0, Rest1};
        _ ->
            <<Key0:KeyLen/binary,
              ValueLen0:32/big-signed-integer,
              Rest1/binary>> = Rest0,
            {Key0, 26+KeyLen, ValueLen0, Rest1}
    end,

    {Value, SizeCount1, Rest4} = case ValueLen of
        -1 ->
            {null, SizeCount0, Rest2};
        _ ->
            <<Value0:ValueLen/binary, Rest3/binary>> = Rest2,
            {Value0, SizeCount0+ValueLen, Rest3}
    end,

    Acc1 = [{Offset, MessageSize, Crc, MagicByte, Attributes, Key, Value}|Acc0],
    message_set_int(Rest4, RestCount-SizeCount1, Acc1).

