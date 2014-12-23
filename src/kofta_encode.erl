-module(kofta_encode).

-export([
    string/1,
    bytes/1,
    array/2,
    request/5,
    message_set/1
]).

-spec string(ErlangBinary) -> EncodedString when
    ErlangBinary :: binary(),
    EncodedString :: binary().

string(String) ->
    Length = byte_size(String),
    <<Length:16/big-signed-integer, String/binary>>.


-spec bytes(ErlangBinary) -> EncodedBytes when
    ErlangBinary :: binary() | null,
    EncodedBytes :: binary().

bytes(null) ->
    <<-1:32/big-signed-integer>>;
bytes(Binary) ->
    Length = byte_size(Binary),
    <<Length:32/big-signed-integer, Binary/binary>>.


-spec array(Encoder, ArrayToEncode) -> EncodedArray when
    Encoder :: fun(),
    ArrayToEncode :: [any()],
    EncodedArray :: iodata().

array(EncodeElement, Array) ->
    Length = length(Array),
    Encoded = [EncodeElement(E) || E <- Array],
    [<<Length:32/big-signed-integer>>|Encoded].


-spec request(APIKey, APIVersion, CorrelationID, ClientID, Msg) -> Response when
    APIKey :: integer(),
    APIVersion :: integer(),
    CorrelationID :: integer(),
    ClientID :: string(),
    Msg :: iodata(),
    Response :: iodata().

request(APIKey, APIVersion, CorrelationID, ClientID, Message) ->
    ClientIDBin = string(ClientID),
    Size = iolist_size(Message) + 8 + byte_size(ClientIDBin),
    [<<Size:32/big-signed-integer,
       APIKey:16/big-signed-integer,
       APIVersion:16/big-signed-integer,
       CorrelationID:32/big-signed-integer>>,
     ClientIDBin,
     Message].


-spec message_set([{Key, Value}]) -> MessageSet when
    Key :: binary() | null,
    Value :: binary() | null,
    MessageSet :: iolist().

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
