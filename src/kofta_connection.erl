-module(kofta_connection).

-behaviour(gen_server).

-export([start_link/2]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(state, {
    host,
    port,
    sock
}).

start_link(Host, Port) ->
    LHost = binary_to_list(Host),
    case gen_tcp:connect(LHost, Port, [binary, {packet, 0}, {active, false}]) of
        {ok, Sock} ->
            % It'd probably be good to use something other than uuids here
            UUID = uuid:to_string(uuid:uuid4()),
            Name = atom_to_list(?MODULE) ++ "_" ++ UUID,
            gen_server:start_link({local, list_to_atom(Name)}, ?MODULE, [LHost, Port, Sock], []);
        {error, Reason} ->
            {error, Reason}
    end.

init([Host, Port, Sock]) ->
    State = #state{
        host=Host,
        port=Port,
        sock=Sock
    },
    {ok, State}.

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
