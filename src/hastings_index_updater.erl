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

-module(hastings_index_updater).


-export([
    update/2,
    load_docs/2
]).


-define(CQS, couch_query_servers).


-record(acc, {
    idx_pid,
    db,
    proc,
    changes_done = 0,
    total_changes,
    prev_cp = os:timestamp(),
    name
}).


-include_lib("couch/include/couch_db.hrl").
-include("hastings.hrl").


update(IndexPid, Index) ->
    #h_idx{
        pid = Pid,
        dbname = DbName,
        ddoc_id = DDocId,
        name = IndexName,
        lang = Language,
        update_seq = UpSeq,
        sig = _Sig
    } = Index,
    erlang:put(io_priority, {view_update, DbName, IndexName}),
    {ok, Db} = couch_db:open_int(DbName, []),
    try
        %% compute on all docs modified since we last computed.
        TotalUpdateChanges = couch_db:count_changes_since(Db, UpSeq),

        IdxPurgeSeq = hastings_util:get_idx_purge_seq(DbName, Pid),
        TotalPurgeChanges = count_pending_purged_docs_since(Db, IdxPurgeSeq),
        TotalChanges = TotalUpdateChanges + TotalPurgeChanges,
        couch_task_status:add_task([
            {type, geo_search_indexer},
            {user, cloudant_util:customer_name(Db)},
            {database, DbName},
            {design_document, DDocId},
            {index, IndexName},
            {progress, 0},
            {changes_done, 0},
            {total_changes, TotalChanges}
        ]),

        %% update status every half second
        couch_task_status:set_update_frequency(500),

        purge_index(DbName, Db, IndexPid, Index, IdxPurgeSeq),

        NewSeq = couch_db:get_update_seq(Db),
        Proc = ?CQS:get_os_process(Language),
        [Changes] = couch_task_status:get([changes_done]),
        try
            Args = [<<"add_fun">>, Index#h_idx.def],
            true = ?CQS:proc_prompt(Proc, Args),
            EnumFun = fun ?MODULE:load_docs/2,
            Acc0 = #acc{
                idx_pid = IndexPid,
                db = Db,
                proc = Proc,
                changes_done = Changes,
                total_changes = TotalChanges,
                name = index_name(DbName, DDocId, IndexName)
            },
            {ok, _} = couch_db:fold_changes(Db, UpSeq, EnumFun, Acc0),
            hastings_index:set_update_seq(IndexPid, NewSeq)
        after
            ?CQS:ret_os_process(Proc)
        end,
        exit({updated, NewSeq})
    after
        couch_db:close(Db)
    end.


load_docs(FDI, Acc) ->
    ChangesDone = Acc#acc.changes_done,
    Total = Acc#acc.total_changes,
    Progress = if Total == 0 -> 0; true -> (ChangesDone * 100) div Total end,
    couch_task_status:update([
            {changes_done, ChangesDone},
            {progress, Progress}
    ]),

    DI = couch_doc:to_doc_info(FDI),
    #doc_info{id=Id, high_seq=Seq, revs=[#rev_info{deleted=Del}|_]} = DI,
    case Del of
        true ->
            ok = hastings_index:remove(Acc#acc.idx_pid, Id);
        false ->
            {ok, Doc} = couch_db:open_doc(Acc#acc.db, DI, []),
            couch_stats:increment_counter([geo, index, doc_count], 1),
            Args = [<<"st_index_doc">>, couch_doc:to_json_obj(Doc, [])],
            [Geoms0] = ?CQS:proc_prompt(Acc#acc.proc, Args),
            Geoms = [G || [G, _Opts] <- Geoms0],
            try
                case Geoms of
                    [] ->
                        ok = hastings_index:remove(Acc#acc.idx_pid, Id);
                    _  ->
                        ok = hastings_index:update(Acc#acc.idx_pid, Id, Geoms)
                end
            catch T:R ->
                % If we failed to update the index then we'll try and
                % remove the geometry at least. If we get an error here
                % we're pretty much screwed anyway so hopefully the
                % log message will give us enough info to be able to fix
                % the bug.
                if length(Geoms) == 0 -> ok; true ->
                    catch hastings_index:remove(Acc#acc.idx_pid, Id)
                end,
                ErrArgs = [Id, Acc#acc.name, {T, R}],
                couch_log:warning("Error updating ~p for ~p :: ~w", ErrArgs)
            end
    end,

    % Force a checkpoint every minute
    case timer:now_diff(Now = os:timestamp(), Acc#acc.prev_cp) >= 60000000 of
        true ->
            ok = hastings_index:set_update_seq(Acc#acc.idx_pid, Seq),
            {ok, Acc#acc{changes_done = ChangesDone + 1, prev_cp = Now}};
        false ->
            {ok, Acc#acc{changes_done = ChangesDone + 1}}
    end.

purge_index(DbName, Db, IndexPid, Index, IdxPurgeSeq) ->
    #h_idx{
        dbname = DbName,
        ddoc_id = DDocId,
        name = IndexName,
        sig = Sig
    } = Index,
    FoldFun = fun({PurgeSeq, _UUId, Id, _Revs}, _Acc) ->
        hastings_index:remove(IndexPid, Id),
        hastings_index:set_purge_seq(IndexPid, PurgeSeq),
        update_task(1),
        {ok, PurgeSeq}
    end,
    {ok, PSeq} = couch_db:fold_purge_infos(
        Db, IdxPurgeSeq, FoldFun, nil, []
    ),
    if PSeq == nil -> ok; true ->
        hastings_util:update_local_purge_doc(
            Db, DbName, DDocId, IndexName, Sig, PSeq
        )
    end.


index_name(DbName, DDocId, IndexName) ->
    <<DbName/binary, " ", DDocId/binary, " ", IndexName/binary>>.


count_pending_purged_docs_since(Db, IdxPurgeSeq) ->
    DbPurgeSeq = couch_db:get_purge_seq(Db),
    DbPurgeSeq - IdxPurgeSeq.


update_task(NumChanges) ->
    [Changes, Total] = couch_task_status:get([changes_done, total_changes]),
    Changes2 = Changes + NumChanges,
    Progress = case Total of
        0 ->
            0;
        _ ->
            (Changes2 * 100) div Total
    end,
    couch_task_status:update([{progress, Progress}, {changes_done, Changes2}]).
