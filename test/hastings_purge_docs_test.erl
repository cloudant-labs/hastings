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
-include_lib("mem3/include/mem3.hrl").
-include_lib("hastings/src/hastings.hrl").


-define(TIMEOUT, 10000).


setup() ->
    {ok, Db} = hastings_test_util:init_db(?tempdb(), 5),
    meck:new(hastings_util, [passthrough]),
    meck:expect(hastings_util, ensure_local_purge_docs, fun(A, B) ->
        meck:passthrough([A, B])
    end),
    meck:new(hastings_rpc, [non_strict, passthrough]),
    meck:expect(hastings_rpc, reply, fun (Msg) -> Msg end),
    Db.


teardown(Db) ->
    couch_db:close(Db),
    couch_server:delete(couch_db:name(Db), [?ADMIN_CTX]),
    meck:unload(hastings_util),
    meck:unload(hastings_rpc),
    ok.


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
                    fun test_purge_remove_local_purge_doc/1,
                    fun test_purge_hook_before_compaction/1
                ]
            }
        }
    }.


test_purge_single(Db) ->
    ?_test(begin
        {ok, Hits} = run_hastings_search(Db),
        ?assertEqual(5, lists:flatlength(Hits)),
        DocIds = [
            <<"point1">>
        ],
        purge_docs(Db, DocIds),
        {ok, Db2} = couch_db:reopen(Db),
        {ok, Hits1} = run_hastings_search(Db2),
        ?assertEqual(4, lists:flatlength(Hits1)),

        ok
    end).

test_purge_multiple(Db) ->
    ?_test(begin
        {ok, Hits} = run_hastings_search(Db),
        ?assertEqual(5, lists:flatlength(Hits)),

        DocIds = [
            <<"point1">>,
            <<"point2">>,
            <<"point3">>,
            <<"point4">>
        ],
        purge_docs(Db, DocIds),
        {ok, Db2} = couch_db:reopen(Db),
        {ok, Hits1} = run_hastings_search(Db2),
        ?assertEqual(1, lists:flatlength(Hits1)),

        ok
    end).

test_purge_multiple2(Db) ->
    ?_test(begin
        {ok, Hits} = run_hastings_search(Db),
        ?assertEqual(5, lists:flatlength(Hits)),

        DocIds1 = [
            <<"point1">>,
            <<"point2">>
        ],
        purge_docs(Db, DocIds1),
        {ok, Db2} = couch_db:reopen(Db),
        {ok, Hits1} = run_hastings_search(Db2),
        ?assertEqual(3, lists:flatlength(Hits1)),

        DocIds2 = [
            <<"point3">>,
            <<"point4">>
        ],
        purge_docs(Db, DocIds2),
        {ok, Db3} = couch_db:reopen(Db2),
        {ok, Hits2} = run_hastings_search(Db3),
        ?assertEqual(1, lists:flatlength(Hits2)),

        ok
    end).

test_purge_local_purge_doc(Db) ->
    ?_test(begin
        {ok, Hits} = run_hastings_search(Db),
        ?assertEqual(5, lists:flatlength(Hits)),

        DocIds1 = [
            <<"point1">>
        ],

        {ok, DDoc} = couch_db:open_doc(Db, <<"_design/geodd">>, []),
        {ok, Idx} = hastings_index:design_doc_to_index(DDoc, <<"geoidx">>),
        {ok, Db1} = couch_db:reopen(Db),

        {ok, LocalPurgeDoc0} = couch_db:open_doc(
            Db1,
            hastings_util:get_local_purge_doc_id(Idx#h_idx.sig),
            []
        ),
        {Props} = couch_doc:to_json_obj(LocalPurgeDoc0, []),
        Rev1 = couch_util:get_value(<<"_rev">>, Props),
        ?assertEqual(<<"0-1">>, Rev1),
        PurgeSeq = couch_util:get_value(<<"purge_seq">>, Props),
        ?assertEqual(0, PurgeSeq),

        purge_docs(Db1, DocIds1),

        {ok, Hits1} = run_hastings_search(Db1),
        ?assertEqual(4, lists:flatlength(Hits1)),

        {ok, Db2} = couch_db:reopen(Db1),

        {ok, LocalPurgeDoc1} = couch_db:open_doc(
            Db2,
            hastings_util:get_local_purge_doc_id(Idx#h_idx.sig),
            []
        ),
        {Props1} = couch_doc:to_json_obj(LocalPurgeDoc1, []),
        Rev1 = couch_util:get_value(<<"_rev">>, Props1),
        ?assertEqual(<<"0-1">>, Rev1),
        PurgeSeq2 = couch_util:get_value(<<"purge_seq">>, Props1),
        ?assertEqual(1, PurgeSeq2),

        DocIds2 = [
            <<"point2">>
        ],
        purge_docs(Db2, DocIds2),

        {ok, Hits2} = run_hastings_search(Db2),
        ?assertEqual(3, lists:flatlength(Hits2)),

        {ok, Db3} = couch_db:reopen(Db2),
        {ok, LocalPurgeDoc2} = couch_db:open_doc(
            Db3,
            hastings_util:get_local_purge_doc_id(Idx#h_idx.sig),
            []
        ),
        {Props2} = couch_doc:to_json_obj(LocalPurgeDoc2, []),
        Rev2 = couch_util:get_value(<<"_rev">>, Props2),
        ?assertEqual(<<"0-1">>, Rev2),

        DocIds3 = [
            <<"point3">>
        ],
        purge_docs(Db3, DocIds3),

        {ok, Hits3} = run_hastings_search(Db3),
        ?assertEqual(2, lists:flatlength(Hits3)),

        {ok, Db4} = couch_db:reopen(Db3),
        {ok, LocalPurgeDoc3} = couch_db:open_doc(
            Db4,
            hastings_util:get_local_purge_doc_id(Idx#h_idx.sig),
            []
        ),
        {Prop3} = couch_doc:to_json_obj(LocalPurgeDoc3, []),
        Rev3 = couch_util:get_value(<<"_rev">>, Prop3),
        ?assertEqual(<<"0-1">>, Rev3),

        ok
    end).

test_purge_verify_index(Db) ->
    ?_test(begin
        {ok, Hits} = run_hastings_search(Db),
        ?assertEqual(5, lists:flatlength(Hits)),

        DocIds1 = [
            <<"point1">>,
            <<"point2">>
        ],
        purge_docs(Db, DocIds1),

        {ok, Hits1} = run_hastings_search(Db),
        ?assertEqual(3, lists:flatlength(Hits1)),

        {ok, DDoc} = couch_db:open_doc(Db, <<"_design/geodd">>, []),
        {ok, Idx} = hastings_index:design_doc_to_index(DDoc, <<"geoidx">>),
        {ok, Db2} = couch_db:reopen(Db),
        {ok, LocPurgeDoc} = couch_db:open_doc(
            Db2,
            hastings_util:get_local_purge_doc_id(Idx#h_idx.sig),
            []
        ),
        #doc{body = {Props}} = LocPurgeDoc,
        ShardName = couch_db:name(Db),
        ?assertEqual(true, hastings_util:verify_index_exists(
            ShardName, Props)),

        ok
    end).

test_purge_remove_local_purge_doc(Db) ->
    ?_test(begin
        {ok, Hits} = run_hastings_search(Db),
        ?assertEqual(5, lists:flatlength(Hits)),
        
        DocIds1 = [
            <<"point1">>,
            <<"point2">>
        ],
        purge_docs(Db, DocIds1),
        
        {ok, Hits1} = run_hastings_search(Db),
        ?assertEqual(3, lists:flatlength(Hits1)),
        
        {ok, DDoc} = couch_db:open_doc(Db, <<"_design/geodd">>, []),
        {ok, Idx} = hastings_index:design_doc_to_index(DDoc, <<"geoidx">>),
        {ok, Db2} = couch_db:reopen(Db),
        {ok, LocalPurgeDoc} = couch_db:open_doc(
            Db2,
            hastings_util:get_local_purge_doc_id(Idx#h_idx.sig),
            []
        ),
        
        {ok, _} = couch_db:update_doc(
            Db2,
            LocalPurgeDoc#doc{deleted=true},
            [?ADMIN_CTX]
        ),
        {ok, Db3} = couch_db:reopen(Db2),
        Result = couch_db:open_doc(
            Db3,
            hastings_util:get_local_purge_doc_id(Idx#h_idx.sig),
            []
        ),
        ?assertEqual({not_found,missing}, Result),
        
        ok
    end).


test_purge_hook_before_compaction(Db) ->
    ?_test(begin
        {ok, Hits} = run_hastings_search(Db),
        ?assertEqual(5, lists:flatlength(Hits)),

        purge_docs(Db, [<<"point1">>]),
        {ok, Hits1} = run_hastings_search(Db),
        ?assertEqual(4, lists:flatlength(Hits1)),

        {ok, #doc{body = {Props1}}} = get_local_purge_doc(Db),
        ?assertEqual(1, couch_util:get_value(<<"purge_seq">>, Props1)),

        {ok, _} = couch_db:start_compact(Db),
        ShardName = couch_db:name(Db),
        wait_compaction(ShardName, ?LINE),

        ?assertEqual(ok, meck:wait(1, hastings_util,
            ensure_local_purge_docs, '_', 5000)
        ),

        % Make sure compaction didn't change the update seq
        {ok, #doc{body = {Props1}}} = get_local_purge_doc(Db),
        ?assertEqual(1, couch_util:get_value(<<"purge_seq">>, Props1)),

        purge_docs(Db, [<<"point2">>]),

        {ok, _} = couch_db:start_compact(Db),
        wait_compaction(ShardName, ?LINE),

        ?assertEqual(ok, meck:wait(2, hastings_util,
            ensure_local_purge_docs, '_', 5000)
        ),

        % Make sure compaction after a purge didn't overwrite
        % the local purge doc for the index
        {ok, #doc{body = {Props2}}} = get_local_purge_doc(Db),
        ?assertEqual(1, couch_util:get_value(<<"purge_seq">>, Props2)),

        % Force another update to ensure that we update
        % the local doc appropriate after compaction
        {ok, Hits2} = run_hastings_search(Db),
        ?assertEqual(3, lists:flatlength(Hits2)),

        {ok, #doc{body = {Props3}}} = get_local_purge_doc(Db),
        ?assertEqual(2, couch_util:get_value(<<"purge_seq">>, Props3)),

        % Check that if the local doc doesn't exist that one
        % is created for the index on compaction
        delete_local_purge_doc(Db),
        ?assertMatch({not_found, _}, get_local_purge_doc(Db)),

        {ok, _} = couch_db:start_compact(Db),
        wait_compaction(ShardName, ?LINE),

        ?assertEqual(ok, meck:wait(3, hastings_util,
            ensure_local_purge_docs, '_', 5000)
        ),

        {ok, #doc{body = {Props4}}} = get_local_purge_doc(Db),
        ?assertEqual(2, couch_util:get_value(<<"purge_seq">>, Props4))
    end).


get_local_purge_doc(Db) ->
    {ok, DDoc} = couch_db:open_doc(Db, <<"_design/geodd">>, []),
    {ok, Idx} = hastings_index:design_doc_to_index(DDoc, <<"geoidx">>),
    DocId = hastings_util:get_local_purge_doc_id(Idx#h_idx.sig),
    {ok, Db2} = couch_db:reopen(Db),
    couch_db:open_doc(Db2, DocId, []).


delete_local_purge_doc(Db) ->
    {ok, DDoc} = couch_db:open_doc(Db, <<"_design/geodd">>, []),
    {ok, Idx} = hastings_index:design_doc_to_index(DDoc, <<"geoidx">>),
    DocId = hastings_util:get_local_purge_doc_id(Idx#h_idx.sig),
    NewDoc = #doc{id = DocId, deleted = true},
    {ok, Db2} = couch_db:reopen(Db),
    {ok, _} = couch_db:update_doc(Db2, NewDoc, []).

wait_compaction(DbName, Line) ->
    WaitFun = fun() ->
        case is_compaction_running(DbName) of
            true -> wait;
            false -> ok
        end
    end,
    case test_util:wait(WaitFun, 10000) of
        timeout ->
            erlang:error({assertion_failed, [
                {module, ?MODULE},
                {line, Line},
                {reason, "Timeout waiting for database compaction"}
            ]});
        _ ->
            ok
    end.


is_compaction_running(DbName) ->
    {ok, DbInfo} = couch_util:with_db(DbName, fun(Db) ->
        couch_db:get_db_info(Db)
    end),
    couch_util:get_value(compact_running, DbInfo).


run_hastings_search(Db) ->
    ShardName = couch_db:name(Db),
    ShardRange = [0,4294967295],
    {ok, DDocInfo} = couch_db:open_doc(Db, <<"_design/geodd">>, []),
    HQArgs0 = #h_args{geom = {bbox,[-180.0,-90.0,180.0,90.0]}},
    hastings_rpc:search(ShardName, ShardRange, DDocInfo, <<"geoidx">>, HQArgs0).


get_rev(#full_doc_info{} = FDI) ->
    #doc_info{
        revs = [#rev_info{} = PrevRev | _]
    } = couch_doc:to_doc_info(FDI),
    PrevRev#rev_info.rev.


purge_docs(Db, DocIds) ->
    lists:foreach(fun(DocId) ->
        FDI = couch_db:get_full_doc_info(Db, DocId),
        Rev = get_rev(FDI),
        Uuid = couch_uuids:random(),
        PurgeInfos = [{Uuid, DocId, [Rev]}],
        {ok, _} = couch_db:purge_docs(Db, PurgeInfos)
    end, DocIds).
