%% Copyright 2014 Cloudant

-module(hastings_format).

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [{hits_to_json,2}];

behaviour_info(_Other) ->
    undefined.