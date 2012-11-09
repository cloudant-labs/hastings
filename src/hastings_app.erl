%% -*- erlang-indent-level: 4;indent-tabs-mode: nil -*-
%% Copyright 2012 Cloudant

-module(hastings_app).
-behaviour(application).
-export([start/2, stop/1]).

start(_Type, []) ->
    hastings_sup:start_link().

stop([]) ->
    ok.
