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
    opts,
    sock,
    reqtime
}).

-include("kofta.hrl").


-spec request(Host, Port, Msg) -> Response when
    Host :: binary(),
    Port :: integer(),
    Msg :: binary(),
    Response :: {ok, binary()} | {error, atom()}.

request(Host, Port, Msg) ->
    {ok, Timeout} = application:get_env(kofta, request_timeout),
    request(Host, Port, Msg, Timeout).


-spec request(Host, Port, Msg, Timeout) -> Response when
    Host :: binary(),
    Port :: integer(),
    Msg :: binary(),
    Timeout :: integer(),
    Response :: {ok, binary()} | {error, atom()}.

request(Host, Port, Msg, Timeout) ->
    StartTime = os:timestamp(),
    Response = request(StartTime, Host, Port, Msg, 50, Timeout),
    TDelta = timer:now_diff(os:timestamp(), StartTime) div 1000,
    ?UPDATE_HISTOGRAM([kofta, requests, latency], TDelta),
    Class = case Response of
        {ok, _} -> success;
        {error, timeout} -> timeout;
        {error, _} -> error
    end,
    ?INCREMENT_COUNTER([kofta, requests, Class]),
    Response.


request(StartTime, Host, Port, Msg, Backoff0, Timeout) when Timeout > 0 ->
    PoolName = kofta_connection:name(Host, Port),
    try poolboy:checkout(PoolName, false, Timeout) of
        full ->
            Backoff1 = trunc(Backoff0 * (1 + random:uniform())),
            timer:sleep(min(Backoff1, Timeout)),
            request(StartTime, Host, Port, Msg, Backoff1, Timeout - Backoff1);
        Worker ->
            TDelta = timer:now_diff(os:timestamp(), StartTime) div 1000,
            ?UPDATE_HISTOGRAM([kofta, requests, checkout_latency], TDelta),
            try
                gen_server:call(Worker, {req, Msg}, Timeout)
            catch exit:{timeout, _} ->
                exit(Worker, kill),
                {error, timeout}
            after
                ok = poolboy:checkin(PoolName, Worker)
            end
    catch exit:{timeout, _} ->
        {error, timeout}
    end;
request(_, _, _, _, _, _) ->
    {error, timeout}.


start_link([Host, Port]) ->
    gen_server:start_link(?MODULE, [binary_to_list(Host), Port], []).


init([Host, Port]) ->
    Opts = [binary, {packet, 0}, {active, false}, {nodelay, true}],
    {ok, #st{host=Host, port=Port, opts=Opts}}.


handle_call(Msg, From, #st{sock=undefined}=State) ->
    #st{host=Host, port=Port, opts=Opts} = State,
    case gen_tcp:connect(Host, Port, Opts) of
        {ok, Socket} ->
            handle_call(Msg, From, State#st{sock=Socket});
        {error, _Reason} ->
            {reply, {error, no_connection}, State}
    end;
handle_call({req, Binary}, _From, #st{sock=Socket}=State) ->
    case gen_tcp:send(Socket, Binary) of
        {error, SendError} ->
            lager:warning(
                "kofta_connection received error on send: ~p",
                [SendError]
            ),
            gen_tcp:close(Socket),
            {reply, {error, failed_to_send}, State#st{sock=undefined}};
        ok ->
            case accumulate_response(Socket) of
                {ok, Data} ->
                    {reply, {ok, Data}, State};
                Else ->
                    gen_tcp:close(Socket),
                    lager:warning(
                        "kofta_connection received error on recv: ~p",
                        [Else]
                    ),
                    {reply, Else, State#st{sock=undefined}}
            end
    end.


handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info(_Msg, State) ->
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


accumulate_response(Socket) ->
    case gen_tcp:recv(Socket, 0) of
        {ok, Data} ->
            <<Size:32/big-signed-integer, Rest/binary>> = Data,
            BytesLeft = Size - byte_size(Rest),
            accumulate_response(Socket, BytesLeft, Data);
        Else ->
            Else
    end.

accumulate_response(_, 0, Acc) ->
    {ok, Acc};
accumulate_response(Socket, BytesLeft0, Acc0) ->
    case gen_tcp:recv(Socket, 0) of
        {ok, Data} ->
            BytesLeft1 = BytesLeft0 - byte_size(Data),
            Acc1 = <<Acc0/binary, Data/binary>>,
            accumulate_response(Socket, BytesLeft1, Acc1);
        Else ->
            Else
    end.


-spec name(Host, Port) -> Name when
    Host :: binary(),
    Port :: integer(),
    Name :: atom().

name(Host, Port) ->
    LHost = binary_to_list(Host),
    LPort = integer_to_list(Port),
    list_to_atom("kofta_connection_pool_" ++ LHost ++ "_" ++ LPort).
