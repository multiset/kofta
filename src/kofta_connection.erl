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

request(Host, Port, Msg) ->
    request(Host, Port, Msg, 5000).

request(Host, Port, Msg, Timeout) ->
    PoolName = kofta_connection:name(Host, Port),
    poolboy:transaction(PoolName, fun(Worker) ->
        Ref = make_ref(),
        Worker ! {'$gen_call', {self(), Ref}, {req, Msg, Timeout}},
        get_rest(Ref, <<>>, Timeout)
    end).

get_rest(Ref, Acc, Timeout) ->
    receive
        {Ref, {cont, Data}} ->
            get_rest(Ref, <<Acc/binary, Data/binary>>, Timeout);
        {Ref, {done, Data}} ->
            {ok, <<Acc/binary, Data/binary>>};
        {_OldRef, _Msg} ->
            get_rest(Ref, Acc, Timeout)
    after Timeout ->
        {error, timeout}
    end.


start_link([Host, Port]) ->
    LHost = binary_to_list(Host),
    gen_server:start_link(?MODULE, [LHost, Port], []).

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

go(#st{sock=undefined}=State) ->
    #st{host=Host, port=Port} = State,
    case open_socket(Host, Port) of
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
                    case accumulate(State, RestBytes, Data) of
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

open_socket(Host, Port) ->
    SockOpts = [binary, {packet, 0}, {active, false}],
    gen_tcp:connect(Host, Port, SockOpts).

accumulate(State, 0, Last) ->
    #st{from=From} = State,
    gen_server:reply(From, {done, Last}),
    NewState = State#st{
        from=undefined,
        request=undefined,
        timeout=undefined,
        last_response=undefined
    },
    {ok, NewState};
accumulate(State, Remaining, Last) ->
    #st{from=From, sock=Sock, timeout=Timeout} = State,
    gen_server:reply(From, {cont, Last}),
    NewState = State#st{last_response=os:timestamp()},
    case gen_tcp:recv(Sock, 0, Timeout) of
        {ok, Data} ->
            accumulate(NewState, Remaining-byte_size(Data), Data);
        {error, Reason} ->
            {error, Reason, NewState}
    end.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(timeout, State) ->
    #st{host=Host, port=Port} = State,
    case open_socket(Host, Port) of
        {ok, Sock} ->
            go(State#st{sock=Sock});
        {error, _Reason} ->
            {noreply, State, 1000}
    end.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

name(Host, Port) ->
    LHost = binary_to_list(Host),
    LPort = integer_to_list(Port),
    list_to_atom("kofta_connection_pool_" ++ LHost ++ "_" ++ LPort).
