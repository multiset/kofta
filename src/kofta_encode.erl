-module(kofta_encode).

-export([
    string/1,
    bytes/1,
    int8/1,
    int16/1,
    int32/1,
    int64/1,
    array/2,
    request/5,
    message_set/1
]).

-spec string(binary()) -> binary().
string(String) ->
    Length = byte_size(String),
    <<Length:16/big-signed-integer, String/binary>>.

-spec bytes(binary() | null) -> iolist().
bytes(null) ->
    <<-1:32/big-signed-integer>>;
bytes(Binary) ->
    Length = byte_size(Binary),
    [<<Length:32/big-signed-integer>>, Binary].

-spec int8(integer()) -> binary().
int8(Int) ->
    <<Int:8/big-signed-integer>>.

-spec int16(integer()) -> binary().
int16(Int) ->
    <<Int:16/big-signed-integer>>.

-spec int32(integer()) -> binary().
int32(Int) ->
    <<Int:32/big-signed-integer>>.

-spec int64(integer()) -> binary().
int64(Int) ->
    <<Int:64/big-signed-integer>>.

-spec array(fun(), [any()]) -> iodata().
array(EncodeElement, Array) ->
    Length = length(Array),
    Encoded = [EncodeElement(E) || E <- Array],
    [<<Length:32/big-signed-integer>>|Encoded].

-spec request(integer(), integer(), integer(), string(), iodata()) -> iodata().
request(APIKey, APIVersion, CorrelationID, ClientID, Message) ->
    ClientIDBin = string(ClientID),
    Size = iolist_size(Message) + 8 + byte_size(ClientIDBin),
    [<<Size:32/big-signed-integer,
       APIKey:16/big-signed-integer,
       APIVersion:16/big-signed-integer,
       CorrelationID:32/big-signed-integer>>,
     ClientIDBin,
     Message].

-spec message_set([{binary() | null, binary() | null}]) -> iolist().
message_set(KVs) ->
    lists:map(fun({Key, Value}) ->
        Body = iolist_to_binary([
            <<0:8/big-signed-integer, 0:8/big-signed-integer>>,
            bytes(Key),
            bytes(Value)
        ]),
        CRC32 = erlang:crc32(Body),
        Message = <<CRC32:32/big-signed-integer, Body/binary>>,
        <<31337:64/big-signed-integer,
          (byte_size(Message)):32/big-signed-integer,
          Message/binary>>
    end, KVs).
