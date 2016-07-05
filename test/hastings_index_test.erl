-module (hastings_index_test).

-include_lib("eunit/include/eunit.hrl").

design_doc_to_index_int_test() ->
    meck:new(couch_util),
    meck:expect(couch_util, get_value, fun
        (_, [<<"p1">>]) -> {[]};
        (_, [<<"p2">>]) -> {[{<<"p2">>, <<"broken">>}]}
    end),
    ?assertThrow({not_found, <<"Geospatial index 'p1' not found">>},
        hastings_index:design_doc_to_index_int(id, [<<"p1">>], <<"p1">>)
    ),
    ?assertThrow({invalid_index, <<"Geospatial index 'p2' is invalid">>},
        hastings_index:design_doc_to_index_int(id, [<<"p2">>], <<"p2">>)
    ),
    meck:unload(couch_util).
