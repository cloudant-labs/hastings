%% Copyright 2014 Cloudant

-module(hastings_index_updater).


-include_lib("couch/include/couch_db.hrl").
-include("hastings.hrl").


-export([
    update/2,
    load_docs/3
]).


-define(CQS, couch_query_servers).


-record(acc, {
    idx_pid,
    db,
    proc,
    changes_done = 0,
    total_changes,
    prev_cp = os:timestamp()
}).


update(IndexPid, Index) ->
    #h_idx{
        dbname = DbName,
        ddoc_id = DDocId,
        name = IndexName,
        lang = Language,
        update_seq = UpSeq
    } = Index,
    erlang:put(io_priority, {view_update, DbName, IndexName}),
    {ok, Db} = couch_db:open_int(DbName, []),
    try
        %% compute on all docs modified since we last computed.
        TotalChanges = couch_db:count_changes_since(Db, UpSeq),

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

        NewSeq = couch_db:get_update_seq(Db),
        Proc = ?CQS:get_os_process(Language),
        try
            Args = [<<"add_fun">>, Index#h_idx.def],
            true = ?CQS:proc_prompt(Proc, Args),
            EnumFun = fun ?MODULE:load_docs/3,
            Acc0 = #acc{
                idx_pid = IndexPid,
                db = Db,
                proc = Proc,
                total_changes = TotalChanges
            },
            {ok, _, _} = couch_db:enum_docs_since(Db, UpSeq, EnumFun, Acc0, []),
            hastings_index:set_update_seq(IndexPid, NewSeq)
        after
            ?CQS:ret_os_process(Proc)
        end,
        exit({updated, NewSeq})
    after
        couch_db:close(Db)
    end.


load_docs(FDI, _, Acc) ->
    ChangesDone = Acc#acc.changes_done,
    couch_task_status:update([
            {changes_done, ChangesDone},
            {progress, (ChangesDone * 100) div Acc#acc.total_changes}
        ]),

    DI = couch_doc:to_doc_info(FDI),
    #doc_info{id=Id, high_seq=Seq, revs=[#rev_info{deleted=Del}|_]} = DI,
    case Del of
        true ->
            ok = hastings_index:remove(Acc#acc.idx_pid, Id);
        false ->
            {ok, Doc} = couch_db:open_doc(Acc#acc.db, DI, []),
            Args = [<<"st_index_doc">>, couch_doc:to_json_obj(Doc, [])],
            [Geoms0] = ?CQS:proc_prompt(Acc#acc.proc, Args),
            Geoms = [G || {G, _Opts} <- Geoms0],
            case Geoms of
                [] -> ok = hastings_index:remove(Acc#acc.idx_pid, Id);
                _  -> ok = hastings_index:update(Acc#acc.idx_pid, Id, Geoms)
            end
    end,

    % Force a checkpoint every minute
    case timer:now_diff(Now = os:timestamp(), Acc#acc.prev_cp) >= 60000000 of
        true ->
            ok = hastings_index:checkpoint(Acc#acc.idx_pid, Seq),
            {ok, Acc#acc{changes_done = ChangesDone + 1, prev_cp = Now}};
        false ->
            {ok, Acc#acc{changes_done = ChangesDone + 1}}
    end.
