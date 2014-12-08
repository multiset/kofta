-module(kofta_fetch).

-export([encode/1, decode/1]).

-spec encode([{binary(), [{integer(), integer(), integer()}]}]) -> iodata().
encode(Data) ->
    Header = <<
      -1:32/big-signed-integer,
      0:32/big-signed-integer,
      0:32/big-signed-integer>>,
    Body = kofta_encode:array(fun({Topic, Partitions}) ->
        Encoded = kofta_encode:array(fun({PartID, Offset, MaxBytes}) ->
            <<PartID:32/big-signed-integer,
              Offset:64/big-signed-integer,
              MaxBytes:32/big-signed-integer>>
        end, Partitions),
        [kofta_encode:string(Topic), Encoded]
    end, Data),
    kofta_encode:request(1, 0, 0, <<>>, [Header, Body]).

decode(Binary0) ->
    {Request, Rest0} = kofta_decode:request(Binary0),
    {Array, Rest5} = kofta_decode:array(fun(Binary1) ->
        {Topic, Rest1} = kofta_decode:string(Binary1),
        {Parts, Rest4} = kofta_decode:array(fun(Binary2) ->
            <<Partition:32/big-signed-integer,
              ErrorCode:16/big-signed-integer,
              Highwater:64/big-signed-integer,
              MsgSetSize:32/big-signed-integer,
              Rest2/binary>> = Binary2,
            {MessageSet, Rest3} = kofta_decode:message_set(Rest2, MsgSetSize),
            {{Partition, ErrorCode, Highwater, MessageSet}, Rest3}
        end, Rest1),
        {{Topic, Parts}, Rest4}
    end, Rest0),
    {{Request, Array}, Rest5}.
