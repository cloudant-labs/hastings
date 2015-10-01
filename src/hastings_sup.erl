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
            hastings_vacuum,
            {hastings_vacuum, start_link, []},
            permanent,
            1000,
            worker,
            [hastings_vacuum]
        },
        {
            hastings_index_manager,
            {hastings_index_manager, start_link, []},
            permanent,
            1000,
            worker,
            [hastings_index_manager]
        }
    ],
    {ok, {{one_for_one, 10, 1},
        couch_epi:register_service(hastings_epi, Children)}}.
