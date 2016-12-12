% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(hastings_purge_docs_test).


-include_lib("couch/include/couch_eunit.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("hastings/src/hastings.hrl").


-define(TIMEOUT, 10000).


setup() ->
    DbName = ?tempdb(),
    ok = fabric:create_db(DbName, [?ADMIN_CTX]),
    meck:new(hastings_index, [passthrough]),
    meck:expect(hastings_index, reset, fun(A) ->
        meck:passthrough([A])
    end),
    DbName.


teardown(DbName) ->
    (catch meck:unload(hastings_index)),
    ok = fabric:delete_db(DbName, [?ADMIN_CTX]).


hastings_purge_test_() ->
    {
        "Hastings Purge Tests",
        {
            setup,
            fun() -> test_util:start_couch([fabric, mem3, hastings]) end,
            fun test_util:stop/1,
            {
                foreach,
                fun setup/0, fun teardown/1,
                [
                    fun test_purge_single/1,
                    fun test_purge_multiple/1,
                    fun test_purge_multiple2/1,
                    fun test_purge_reset/1
                ]
            }
        }
    }.


test_purge_single(DbName) ->
    ?_test(begin
        Docs1 = hastings_test_util:make_docs(5),
        {ok, _} = fabric:update_docs(DbName, Docs1, [?ADMIN_CTX]),
        {ok, _} = fabric:update_doc(DbName, hastings_test_util:ddoc(geo), [?ADMIN_CTX]),

        {ok, Hits} = run_hastings_search(DbName),
        ?assertEqual(5, lists:flatlength(Hits)),

        % open couch database using couch_db:open_init/2 API
        % in order to use couch_db:purge_docs/2;
        % this will be replaced with fabric:purge_docs/3 later
        Suffix = lists:sublist(binary_to_list(DbName), 15, 10),
        DBFullName = "shards/00000000-ffffffff/" ++ binary_to_list(DbName) ++ "." ++ Suffix,
        {ok, Db} = couch_db:open_int(list_to_binary(DBFullName), []),

        purge_docs(DbName, Db, [<<"point1">>]),
        {ok, Hits1} = run_hastings_search(DbName),
        ?assertEqual(4, lists:flatlength(Hits1)),

        ok
    end).

test_purge_multiple(DbName) ->
    ?_test(begin
        Docs1 = hastings_test_util:make_docs(5),
        {ok, _} = fabric:update_docs(DbName, Docs1, [?ADMIN_CTX]),
        {ok, _} = fabric:update_doc(DbName, hastings_test_util:ddoc(geo), [?ADMIN_CTX]),

        {ok, Hits} = run_hastings_search(DbName),
        ?assertEqual(5, lists:flatlength(Hits)),

        % open couch database using couch_db:open_init/2 API
        % in order to use couch_db:purge_docs/2;
        % this will be replaced with fabric:purge_docs/3 later
        Suffix = lists:sublist(binary_to_list(DbName), 15, 10),
        DBFullName = "shards/00000000-ffffffff/" ++ binary_to_list(DbName) ++ "." ++ Suffix,
        {ok, Db} = couch_db:open_int(list_to_binary(DBFullName), []),

        DocIds = [
            <<"point1">>,
            <<"point2">>,
            <<"point3">>,
            <<"point4">>
        ],
        purge_docs(DbName, Db, DocIds),

        {ok, Hits1} = run_hastings_search(DbName),
        ?assertEqual(1, lists:flatlength(Hits1)),

        ok
    end).

test_purge_multiple2(DbName) ->
    ?_test(begin
        Docs1 = hastings_test_util:make_docs(5),
        {ok, _} = fabric:update_docs(DbName, Docs1, [?ADMIN_CTX]),
        {ok, _} = fabric:update_doc(DbName, hastings_test_util:ddoc(geo), [?ADMIN_CTX]),

        {ok, Hits} = run_hastings_search(DbName),
        ?assertEqual(5, lists:flatlength(Hits)),

        % open couch database using couch_db:open_init/2 API
        % in order to use couch_db:purge_docs/2;
        % this will be replaced with fabric:purge_docs/3 later
        Suffix = lists:sublist(binary_to_list(DbName), 15, 10),
        DBFullName = "shards/00000000-ffffffff/" ++ binary_to_list(DbName) ++ "." ++ Suffix,
        {ok, Db} = couch_db:open_int(list_to_binary(DBFullName), []),

        DocIds1 = [
            <<"point1">>,
            <<"point2">>
        ],
        purge_docs(DbName, Db, DocIds1),

        {ok, Hits1} = run_hastings_search(DbName),
        ?assertEqual(3, lists:flatlength(Hits1)),

        DocIds2 = [
            <<"point3">>,
            <<"point4">>
        ],
        purge_docs(DbName, Db, DocIds2),

        {ok, Hits2} = run_hastings_search(DbName),
        ?assertEqual(1, lists:flatlength(Hits2)),

        ok
    end).

test_purge_reset(DbName) ->
    ?_test(begin
        Docs1 = hastings_test_util:make_docs(10),
        {ok, _} = fabric:update_docs(DbName, Docs1, [?ADMIN_CTX]),
        {ok, _} = fabric:update_doc(DbName, hastings_test_util:ddoc(geo), [?ADMIN_CTX]),

        % open couch database using couch_db:open_init/2 API
        % in order to use couch_db:purge_docs/2;
        % this will be replaced with fabric:purge_docs/3 later
        Suffix = lists:sublist(binary_to_list(DbName), 15, 10),
        DBFullName = "shards/00000000-ffffffff/" ++ binary_to_list(DbName) ++ "." ++ Suffix,
        {ok, Db} = couch_db:open_int(list_to_binary(DBFullName), [?ADMIN_CTX]),
        couch_db:set_purged_docs_limit(Db, 3),

        {ok, Hits} = run_hastings_search(DbName),
        ?assertEqual(10, lists:flatlength(Hits)),

        ok = meck:reset(hastings_index),
        DocIds1 = [
            <<"point1">>,
            <<"point2">>
        ],
        purge_docs(DbName, Db, DocIds1),
        {ok, Hits1} = run_hastings_search(DbName),
        ?assertEqual(8, lists:flatlength(Hits1)),
        ?assertEqual(false, meck:called(hastings_index, reset, '_')),

        ok = meck:reset(hastings_index),
        DocIds2 = [
            <<"point3">>,
            <<"point9">>,
            <<"point5">>,
            <<"point6">>
        ],
        purge_docs(DbName, Db, DocIds2),
        {ok, Hits2}  = run_hastings_search(DbName),
        ?assertEqual(4, lists:flatlength(Hits2)),
        ?assert(meck:called(hastings_index, reset, '_')),

        ok = meck:reset(hastings_index),
        DocIds3 = [
            <<"point7">>,
            <<"point8">>
        ],
        purge_docs(DbName, Db, DocIds3),
        {ok, Hits3}= run_hastings_search(DbName),
        ?assertEqual(2, lists:flatlength(Hits3)),
        ?assertEqual(false, meck:called(hastings_index, reset, '_')),

        ok
    end).


run_hastings_search(DbName) ->
    HQArgs = #h_args{geom = {bbox,[-180.0,-90.0,180.0,90.0]}},
    {ok, DDoc} = fabric:open_doc(DbName, <<"_design/geodd">>, []),
    hastings_fabric_search:go(DbName, DDoc, <<"geoidx">>, HQArgs).


get_rev(#full_doc_info{} = FDI) ->
    #doc_info{
        revs = [#rev_info{} = PrevRev | _]
    } = couch_doc:to_doc_info(FDI),
    PrevRev#rev_info.rev.


purge_docs(DbName, Db, DocIds) ->
    lists:foreach(fun(DocId) ->
        FDI = fabric:get_full_doc_info(DbName, DocId, []),
        Rev = get_rev(FDI),
        {ok, {_, [{ok, _}]}} = couch_db:purge_docs(Db, [{DocId, [Rev]}])
     end, DocIds).
