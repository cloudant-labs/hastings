%% Copyright 2014 Cloudant

-module(hastings_sup).
-behaviour(supervisor).


-export([
    start_link/0,
    init/1
]).


start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


init(_Args) ->
    Children = [
        {
            hastings_index_manager,
            {hastings_index_manager, start_link, []},
            permanent,
            1000,
            worker,
            [hastings_index_manager]
        }
    ],
    {ok, {{one_for_one, 10, 1}, Children}}.
