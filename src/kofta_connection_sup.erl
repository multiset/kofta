-module(kofta_connection_sup).

-behaviour(supervisor).

-export([start_link/2]).

-export([init/1]).

start_link(Host, Port) ->
    Name = atom_to_list(?MODULE) ++ "_" ++ binary_to_list(Host) ++ "_"
        ++ integer_to_list(Port),
    supervisor:start_link({local, list_to_atom(Name)}, ?MODULE, [Host, Port]).

init([Host, Port]) ->
    Spec = {
        kofta_connection, {kofta_connection, start_link, [Host, Port]},
        transient, 5000, worker, [kofta_connection]
    },
    {ok, {{simple_one_for_one, 5, 10}, [Spec]}}.
