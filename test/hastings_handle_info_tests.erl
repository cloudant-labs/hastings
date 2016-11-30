%% Copyright 2016 Cloudant

-module(hastings_handle_info_tests).


-include_lib("couch/include/couch_eunit.hrl").
-include_lib("couch/include/couch_db.hrl").


-define(TIMEOUT, 10000).


setup() ->
    DbName = ?tempdb(),
    ok = fabric:create_db(DbName, [?ADMIN_CTX]),
    DbName.


teardown(DbName) ->
    ok = fabric:delete_db(DbName, [?ADMIN_CTX]).


handle_info_test_() ->
    {
        "hastings handle info test",
        {
            setup,
            fun() -> test_util:start_couch([ken, fabric, mem3, hastings]) end,
            fun test_util:stop/1,
            {
                foreach,
                fun setup/0, fun teardown/1,
                [
                    fun handle_info_test/1
                ]
            }
        }
    }.


handle_info_test(DbName) ->
    ?_test(begin
        Fn = filename:join([?APPDIR, "test", "testdata", "geo_docs.json"]),
        {ok, Bin} = file:read_file(Fn),
        DocsArray = jiffy:decode(Bin),
        Docs = [couch_doc:from_json_obj(JsonObj) || JsonObj <- DocsArray],

        DesignFn = filename:join([?APPDIR, "test", "testdata",
            "geo_design.json"]),
        {ok, Bin2} = file:read_file(DesignFn),
        JsonDDoc = jiffy:decode(Bin2),
        DDoc = couch_doc:from_json_obj(JsonDDoc),

        {ok, _} = fabric:update_docs(DbName, Docs, [?ADMIN_CTX]),

        Pid = ets:first(hastings_by_pid),
        timer:sleep(1000),

        {R1, R2, St} = hastings_index:handle_info(
            {'EXIT', Pid, {normal, {gen_server, call, [Pid, stop]}}},
                {st, Pid,
                    {h_idx, {rtree, Pid},
                        <<"shards/00000000-1fffffff/eunit-test-db-1477558961664896.1477558961">>,
                        <<"_design/geodd">>, <<"geoidx">>, Bin2,
                        <<"javascript">>, <<"rtree">>,
                        2,4326,0, <<"430182192af06d411d8aa63f0e9b5f4d">>},
            Pid, undefined, [], 0}
        ),
        ?assertEqual(R1, stop),
        ?assertEqual(R2, normal),
        ok
    end).
