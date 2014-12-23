-module(kofta_app).

-behaviour(application).

-export([start/2, stop/1]).


start(_StartType, _StartArgs) ->
    kofta_sup:start_link().


stop(_State) ->
    ok.
