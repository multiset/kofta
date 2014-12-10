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
    name/2
]).

-record(state, {
    host,
    port,
    sock
}).

request(Host, Port, Msg) ->
    PoolName = kofta_connection_pool:name(Host, Port),
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {req, Msg})
    end).

start_link([Host, Port]) ->
    LHost = binary_to_list(Host),
    SockOpts = [binary, {packet, 0}, {active, false}],
    case gen_tcp:connect(LHost, Port, SockOpts) of
        {ok, Sock} ->
            gen_server:start_link(
                ?MODULE,
                [LHost, Port, Sock],
                []
            );
        {error, Reason} ->
            {error, Reason}
    end.

init([Host, Port, Sock]) ->
    {ok, #state{host=Host, port=Port, sock=Sock}}.

handle_call({req, Binary}, _From, State) ->
    go(Binary, State).

go(Binary, State) ->
    #state{
        sock=Sock
    } = State,
    Reply = case gen_tcp:send(Sock, Binary) of
        ok ->
            case gen_tcp:recv(Sock, 0) of
                {ok, Data} ->
                    <<Size:32/big-signed-integer, Rest/binary>> = Data,
                    TotalData = accumulate(Sock, Size-byte_size(Rest), Data),
                    {ok, TotalData};
                {error, RecvError} ->
                    {error, RecvError}
            end;
        {error, SendError} ->
            {error, SendError}
    end,
    {reply, Reply, State}.

accumulate(_Sock, 0, Acc) ->
    Acc;
accumulate(Sock, Remaining, Acc) ->
    case gen_tcp:recv(Sock, 0) of
        {ok, Data} ->
            accumulate(Sock, Remaining-byte_size(Data), <<Acc/binary, Data/binary>>);
        {error, Reason} ->
            {error, Reason}
    end.


handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(timeout, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

name(Host, Port) ->
    LHost = binary_to_list(Host),
    LPort = integer_to_list(Port),
    list_to_atom("kofta_connection_pool_" ++ LHost ++ "_" ++ LPort).
