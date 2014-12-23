-module(kofta_proper).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

prop_string_coding() ->
    ?FORALL(
        Binary,
        binary(),
        begin
            Encoded = kofta_encode:string(Binary),
            true = {Binary, <<>>} =:= kofta_decode:string(Encoded)
        end
    ).


%% TODO: kofta_decode:string/1 won't decode null!
%% -type kofta_bytes() :: binary() | null.
-type kofta_bytes() :: binary().

prop_bytes_coding() ->
    ?FORALL(
        Binary,
        kofta_bytes(),
        begin
            Encoded = kofta_encode:bytes(Binary),
            true = {Binary, <<>>} =:= kofta_decode:bytes(Encoded)
        end
    ).

-type coder() :: string | bytes.

prop_array_coding() ->
    ?FORALL(
        {BinList, Coder},
        {[binary()], coder()},
        begin
            Encoder = fun(I) -> apply(kofta_encode, Coder, [I]) end,
            Decoder = fun(I) -> apply(kofta_decode, Coder, [I]) end,
            EncodedIOList = kofta_encode:array(Encoder, BinList),
            Encoded = iolist_to_binary(EncodedIOList),
            true = {BinList, <<>>} =:= kofta_decode:array(Decoder, Encoded)
        end
    ).

proper_test_() ->
    {
        timeout,
        100000,
        [] = proper:module(?MODULE, [{to_file, user}, {numtests, 1000}])
    }.
