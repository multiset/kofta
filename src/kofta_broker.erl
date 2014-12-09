-module(kofta_broker).

-behaviour(gen_server).

-export([request/2]).

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
    open_connections,
    pending_requests,
    request_backlog,
    host,
    port
}).

request(Broker, Msg) ->
    gen_server:call(Broker, {req, Msg}).

start_link(Host, Port) ->
    LHost = binary_to_list(Host),
    ID = "kofta_broker_" ++ LHost ++ "_" ++ integer_to_list(Port),
    gen_server:start_link({local, list_to_atom(ID)}, ?MODULE, [Host, Port], []).

init([Host, Port]) ->
    supervisor:start_child(kofta_connection_pool_sup, [Host, Port]),
    LHost = binary_to_list(Host),
    Name = "kofta_connection_sup_" ++ LHost ++ "_" ++ integer_to_list(Port),
    Children = lists:map(fun(_) ->
        {ok, Child} = supervisor:start_child(list_to_atom(Name), []),
        Child
    end, lists:seq(1, 10)),
    ets:insert(broker_host_pids, {{Host, Port}, self()}),
    State = #state{
        open_connections=Children,
        host=Host,
        port=Port,
        request_backlog=queue:new()
    },
    {ok, State}.

handle_call({req, Msg}, From, State) ->
    {noreply, maybe_make_request(Msg, From, State)}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({Ref, Msg}, State0) ->
    #state{
        open_connections=OpenConns,
        request_backlog=Backlog,
        pending_requests=Pending
    } = State0,
    State3 = case lists:keytake(Ref, 1, Pending) of
        false ->
            State0;
        {value, {_, OpenConn, From}, NewPending} ->
            gen_server:reply(From, Msg),
            State1 = State0#state{
                open_connections=[OpenConn|OpenConns],
                pending_requests=NewPending
            },
            case queue:out(Backlog) of
                {empty, _} ->
                    State1;
                {{value, {OldFrom, OldMsg}}, NewBacklog} ->
                    State2 = State1#state{
                        request_backlog=NewBacklog
                    },
                    maybe_make_request(OldMsg, OldFrom, State2)
            end
    end,

    {noreply, State3}.

terminate(_Reason, State) ->
    #state{
        host=Host,
        port=Port
    } = State,
    ets:match_delete(broker_host_pids, {{Host, Port}, self()}),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

maybe_make_request(Msg, From, State) ->
    #state{
        open_connections=OpenConns,
        request_backlog=Backlog,
        pending_requests=Pending
    } = State,
    case OpenConns of
        [] ->
            State#state{request_backlog=queue:in({Msg, From}, Backlog)};
        [OpenConn|RestConns] ->
            Ref = make_ref(),
            % Responses are handled in handle_info
            OpenConn ! {'$gen_call', {self(), Ref}, {req, Msg}},
            State#state{
                open_connections=RestConns,
                pending_requests=[{Ref, OpenConn, From}|Pending]
            }
    end.
