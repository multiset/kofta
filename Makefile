SHELL=/bin/bash

all: compile run

clean:
	@./rebar clean

deps:
	@./rebar get-deps

compile:
	@./rebar compile

run:
	@erl -pa deps/*/ebin -pa ebin -s lager -s kofta

test: eunit

eunit:
	@./rebar skip_deps=true eunit
