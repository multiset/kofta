-module(kofta_connection).

-behaviour(gen_server).
-behaviour(poolboy_worker).

-export([start_link/1]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-export([
    request/3,
    request/4,
    name/2
]).

-record(st, {
    host,
    port,
    sock,
    from,
    request,
    timeout,
    last_response
}).

-define(SOCK_OPTS, [binary, {packet, 0}, {active, false}]).


-spec request(Host, Port, Msg) -> Response when
    Host :: binary(),
    Port :: integer(),
    Msg :: binary(),
    Response :: any().

request(Host, Port, Msg) ->
    request(Host, Port, Msg, 5000).


-spec request(Host, Port, Msg, Timeout) -> Response when
    Host :: binary(),
    Port :: integer(),
    Msg :: binary(),
    Timeout :: integer(),
    Response :: any() | {error, atom()}.

request(Host, Port, Msg, Timeout) ->
    PoolName = kofta_connection:name(Host, Port),
    transact(
        PoolName,
        fun(Worker, WorkerTimeout) ->
            Ref = make_ref(),
            Worker ! {'$gen_call', {self(), Ref}, {req, Msg, WorkerTimeout}},
            accumulate_response(Ref, <<>>, WorkerTimeout)
        end,
        50,
        Timeout
    ).

-spec transact(Pool, Fun, Backoff, Timeout) -> Response when
    Pool :: atom(),
    Fun :: fun((pid(), pos_integer()) -> FunResponse),
    Backoff :: pos_integer(),
    Timeout :: non_neg_integer(),
    FunResponse :: any(),
    Response :: FunResponse | {error, timeout}.

transact(Pool, Fun, Backoff, Timeout) when Timeout > 0 ->
    try poolboy:checkout(Pool, false, Timeout) of
        full ->
            timer:sleep(min(Backoff, Timeout)),
            transact(Pool, Fun, Backoff * 2, Timeout - Backoff);
        Worker ->
            try
                Fun(Worker, Timeout)
            catch exit:{timeout, _} ->
                {error, timeout}
            after
                ok = poolboy:checkin(Pool, Worker)
            end
    catch exit:{timeout, _} ->
        {error, timeout}
    end;
transact(_, _, _, _) ->
    {error, timeout}.


-spec accumulate_response(Ref, Acc, Timeout) -> {ok, Response} | Error when
    Ref :: reference(),
    Acc :: binary(),
    Timeout :: integer(),
    Response :: binary(),
    Error :: {error, binary()}.

accumulate_response(Ref, Acc, Timeout) ->
    receive
        {Ref, {cont, Data}} ->
            accumulate_response(Ref, <<Acc/binary, Data/binary>>, Timeout);
        {Ref, {done, Data}} ->
            {ok, <<Acc/binary, Data/binary>>};
        {_OldRef, _Msg} ->
            accumulate_response(Ref, Acc, Timeout)
    after Timeout ->
        {error, timeout}
    end.


start_link([Host, Port]) ->
    gen_server:start_link(?MODULE, [binary_to_list(Host), Port], []).


init([Host, Port]) ->
    {ok, #st{host=Host, port=Port}}.


handle_call({req, Binary, Timeout}, From, State) ->
    NewState = State#st{
        from=From,
        request=Binary,
        timeout=Timeout,
        last_response=os:timestamp()
    },
    go(NewState).


handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info(timeout, State) ->
    #st{host=Host, port=Port} = State,
    case gen_tcp:connect(Host, Port, ?SOCK_OPTS) of
        {ok, Sock} ->
            go(State#st{sock=Sock});
        {error, _Reason} ->
            {noreply, State, 1000}
    end.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


-spec format_response(State) -> {Reply, State} | {Reply, State, Timeout} when
    State :: #st{},
    Reply :: noreply,
    Timeout :: integer().

format_response(State) ->
    #st{
        timeout=MaxTimeout,
        last_response=Last
    } = State,
    case timer:now_diff(Last, os:timestamp()) of
        Diff when Diff >= MaxTimeout ->
            NewState = State#st{
                from=undefined,
                request=undefined,
                timeout=undefined,
                last_response=undefined
            },
            {noreply, NewState};
        _Diff ->
            {noreply, State, 1000}
    end.

-spec go(State) -> {Reply, State} | {Reply, State, Timeout} when
    State :: #st{},
    Reply :: noreply,
    Timeout :: integer().

go(#st{sock=undefined}=State) ->
    #st{host=Host, port=Port} = State,
    case gen_tcp:connect(Host, Port, ?SOCK_OPTS) of
        {ok, Sock} ->
            go(State#st{sock=Sock});
        {error, _Reason} ->
            format_response(State)
    end;

go(State) ->
    #st{
        sock=Sock,
        request=Binary,
        timeout=Timeout
    } = State,
    case gen_tcp:send(Sock, Binary) of
        ok ->
            case gen_tcp:recv(Sock, 0, Timeout) of
                {ok, Data} ->
                    <<Size:32/big-signed-integer, Rest/binary>> = Data,
                    RestBytes = Size-byte_size(Rest),
                    case stream_response(State, RestBytes, Data) of
                        {ok, NewState} ->
                            {noreply, NewState};
                        {error, _Reason, NewState} ->
                            format_response(NewState)
                    end;
                {error, _RecvError} ->
                    format_response(State)
            end;
        {error, _SendError} ->
            format_response(State)
    end.


-spec stream_response(State, BytesToRecv, Prev) -> {ok, State} | Error when
    State :: #st{},
    BytesToRecv :: integer(),
    Prev :: binary(),
    Error :: {error, any(), #st{}}.

stream_response(State, 0, Last) ->
    #st{from=From} = State,
    gen_server:reply(From, {done, Last}),
    NewState = State#st{
        from=undefined,
        request=undefined,
        timeout=undefined,
        last_response=undefined
    },
    {ok, NewState};

stream_response(State, Remaining, Last) ->
    #st{from=From, sock=Sock, timeout=Timeout} = State,
    gen_server:reply(From, {cont, Last}),
    NewState = State#st{last_response=os:timestamp()},
    case gen_tcp:recv(Sock, 0, Timeout) of
        {ok, Data} ->
            stream_response(NewState, Remaining-byte_size(Data), Data);
        {error, Reason} ->
            {error, Reason, NewState}
    end.


-spec name(Host, Port) -> Name when
    Host :: binary(),
    Port :: integer(),
    Name :: atom().

name(Host, Port) ->
    LHost = binary_to_list(Host),
    LPort = integer_to_list(Port),
    list_to_atom("kofta_connection_pool_" ++ LHost ++ "_" ++ LPort).
