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
                    fun test_purge_local_purge_doc/1,
                    fun test_purge_verify_index/1,
                    fun test_purge_remove_local_purge_doc/1
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

        purge_docs(DbName, [<<"point1">>]),
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

        DocIds = [
            <<"point1">>,
            <<"point2">>,
            <<"point3">>,
            <<"point4">>
        ],
        purge_docs(DbName, DocIds),

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

        DocIds1 = [
            <<"point1">>,
            <<"point2">>
        ],
        purge_docs(DbName, DocIds1),

        {ok, Hits1} = run_hastings_search(DbName),
        ?assertEqual(3, lists:flatlength(Hits1)),

        DocIds2 = [
            <<"point3">>,
            <<"point4">>
        ],
        purge_docs(DbName, DocIds2),

        {ok, Hits2} = run_hastings_search(DbName),
        ?assertEqual(1, lists:flatlength(Hits2)),

        ok
    end).

test_purge_local_purge_doc(DbName) ->
    ?_test(begin
        Docs1 = hastings_test_util:make_docs(10),
        {ok, _} = fabric:update_docs(DbName, Docs1, [?ADMIN_CTX]),
        {ok, _} = fabric:update_doc(DbName, hastings_test_util:ddoc(geo), [?ADMIN_CTX]),

        {ok, Hits} = run_hastings_search(DbName),
        ?assertEqual(10, lists:flatlength(Hits)),

        DocIds1 = [
            <<"point1">>,
            <<"point2">>
        ],
        purge_docs(DbName, DocIds1),

        {ok, Hits1} = run_hastings_search(DbName),
        ?assertEqual(8, lists:flatlength(Hits1)),

        {ok, DDoc} = fabric:open_doc(DbName, <<"_design/geodd">>, []),
        {ok, Idx} = hastings_index:design_doc_to_index(DDoc, <<"geoidx">>),

        {ok, LocalPurgeDoc1} = fabric:open_doc(DbName, hastings_util:get_local_purge_doc_id(Idx#h_idx.sig), []),
        {Props1} = couch_doc:to_json_obj(LocalPurgeDoc1, []),
        Rev1 = couch_util:get_value(<<"_rev">>, Props1),
        ?assertEqual(<<"0-1">>, Rev1),

        DocIds2 = [
            <<"point3">>,
            <<"point4">>
        ],
        purge_docs(DbName, DocIds2),

        {ok, Hits2} = run_hastings_search(DbName),
        ?assertEqual(6, lists:flatlength(Hits2)),

        {ok, LocalPurgeDoc2} = fabric:open_doc(DbName, hastings_util:get_local_purge_doc_id(Idx#h_idx.sig), []),
        {Props2} = couch_doc:to_json_obj(LocalPurgeDoc2, []),
        Rev2 = couch_util:get_value(<<"_rev">>, Props2),
        ?assertEqual(<<"0-1">>, Rev2),

        DocIds3 = [
            <<"point5">>,
            <<"point6">>
        ],
        purge_docs(DbName, DocIds3),

        {ok, Hits3} = run_hastings_search(DbName),
        ?assertEqual(4, lists:flatlength(Hits3)),

        {ok, LocalPurgeDoc3} = fabric:open_doc(DbName, hastings_util:get_local_purge_doc_id(Idx#h_idx.sig), []),
        {Prop3} = couch_doc:to_json_obj(LocalPurgeDoc3, []),
        Rev3 = couch_util:get_value(<<"_rev">>, Prop3),
        ?assertEqual(<<"0-1">>, Rev3),

        ok
    end).

test_purge_verify_index(DbName) ->
    ?_test(begin
        Docs1 = hastings_test_util:make_docs(5),
        {ok, _} = fabric:update_docs(DbName, Docs1, [?ADMIN_CTX]),
        {ok, _} = fabric:update_doc(DbName, hastings_test_util:ddoc(geo), [?ADMIN_CTX]),

        {ok, Hits} = run_hastings_search(DbName),
        ?assertEqual(5, lists:flatlength(Hits)),

        DocIds1 = [
            <<"point1">>,
            <<"point2">>
        ],
        purge_docs(DbName, DocIds1),

        {ok, Hits1} = run_hastings_search(DbName),
        ?assertEqual(3, lists:flatlength(Hits1)),

        {ok, DDoc} = fabric:open_doc(DbName, <<"_design/geodd">>, []),
        {ok, Idx} = hastings_index:design_doc_to_index(DDoc, <<"geoidx">>),
        {ok, LocPurgeDoc} = fabric:open_doc(DbName, hastings_util:get_local_purge_doc_id(Idx#h_idx.sig), []),
        {Props} = couch_doc:to_json_obj(LocPurgeDoc,[]),
        {Options} = couch_util:get_value(<<"verify_options">>, Props),

        ?assertEqual(true, hastings_index:verify_index_exists(Options)),
        hastings_vacuum:clean_db(DbName),

        ok
    end).

test_purge_remove_local_purge_doc(DbName) ->
  ?_test(begin
      Docs1 = hastings_test_util:make_docs(5),
      {ok, _} = fabric:update_docs(DbName, Docs1, [?ADMIN_CTX]),
      {ok, _} = fabric:update_doc(DbName, hastings_test_util:ddoc(geo), [?ADMIN_CTX]),

      {ok, Hits} = run_hastings_search(DbName),
      ?assertEqual(5, lists:flatlength(Hits)),

      DocIds1 = [
          <<"point1">>,
          <<"point2">>
      ],
      purge_docs(DbName, DocIds1),

      {ok, Hits1} = run_hastings_search(DbName),
      ?assertEqual(3, lists:flatlength(Hits1)),

      {ok, DDoc} = fabric:open_doc(DbName, <<"_design/geodd">>, []),
      {ok, Idx} = hastings_index:design_doc_to_index(DDoc, <<"geoidx">>),
      {ok, LocalPurgeDoc} = fabric:open_doc(DbName, hastings_util:get_local_purge_doc_id(Idx#h_idx.sig), []),

      {ok, _} = fabric:update_doc(DbName, LocalPurgeDoc#doc{deleted=true}, [?ADMIN_CTX]),

      Result = fabric:open_doc(DbName, hastings_util:get_local_purge_doc_id(Idx#h_idx.sig), []),
      ?assertEqual({not_found,missing}, Result),

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


purge_docs(DbName, DocIds) ->
    lists:foreach(fun(DocId) ->
        FDI = fabric:get_full_doc_info(DbName, DocId, []),
        Rev = get_rev(FDI),
        {ok, {_, [{ok, _}]}} = fabric:purge_docs(DbName, [{DocId, [Rev]}], [])
     end, DocIds).
