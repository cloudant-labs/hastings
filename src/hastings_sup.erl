%% -*- erlang-indent-level: 4;indent-tabs-mode: nil -*-
%% Copyright 2012 Cloudant

-module(hastings_sup).
-behaviour(supervisor).
-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init(_Args) ->
    Children = [
        child(hastings_index_manager)
    ],
    {ok, {{one_for_one,10,1}, Children}}.

child(Child) ->
    {Child, {Child, start_link, []}, permanent, 1000, worker, [Child]}.
