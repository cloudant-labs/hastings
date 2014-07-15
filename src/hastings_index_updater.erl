%% Copyright 2014 Cloudant

-module(hastings_index_updater).


-include_lib("couch/include/couch_db.hrl").
-include("hastings.hrl").


-import(couch_query_servers, [
    get_os_process/1,
    ret_os_process/1,
    proc_prompt/2
]).


-export([
    update/2,
    load_docs/3
]).


update(IndexPid, Index) ->
    #index{
        current_seq = CurSeq,
        dbname = DbName,
        ddoc_id = DDocId,
        name = IndexName
    } = Index,
    erlang:put(io_priority, {view_update, DbName, IndexName}),
    {ok, Db} = couch_db:open_int(DbName, []),
    try
        %% compute on all docs modified since we last computed.
        TotalChanges = couch_db:count_changes_since(Db, CurSeq),

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

        NewCurSeq = couch_db:get_update_seq(Db),
        Proc = get_os_process(<<"javascript">>),
        try
            true = proc_prompt(Proc, [<<"add_fun">>, Index#index.def]),
            EnumFun = fun ?MODULE:load_docs/3,
            Acc0 = {0, IndexPid, Db, Proc, TotalChanges, now()},

            {ok,_,_} = couch_db:enum_docs_since(Db, CurSeq, EnumFun, Acc0, []),
            hastings_index:update_seq(IndexPid, NewCurSeq)
        after
            ret_os_process(Proc)
        end,
        exit({updated, NewCurSeq})
    after
        couch_db:close(Db)
    end.


load_docs(FDI, _, {I, IndexPid, Db, Proc, Total, _LastCommitTime}=Acc) ->
    couch_task_status:update([
            {changes_done, I},
            {progress, (I * 100) div Total}
        ]),
    DI = couch_doc:to_doc_info(FDI),
    #doc_info{id=Id, revs=[#rev_info{deleted=Del}|_]} = DI,

    Options = case Del of
        true ->
            [deleted];
        _ ->
            []
    end,

    case couch_db:open_doc(Db, FDI, Options) of
        {ok, #doc{revs = {1, _}}} ->
            % no previous revisions to delete from spatialindex
            {ok, Doc} = couch_db:open_doc(Db, DI, []),
            Json = couch_doc:to_json_obj(Doc, []),
            case proc_prompt(Proc, [<<"st_index_doc">>, Json]) of
                [[]] ->
                    ok;
                [Features] ->
                    [ok = hastings_index:update(IndexPid, Id, Val)
                            || [Val | _] <- Features]
            end;
        {ok, #doc{revs = {RevPos, [_, PrevRev|_]}} = OldDoc} ->
            {ok, [{ok, PrevDoc}]} = couch_db:open_doc_revs(
                    Db, OldDoc#doc.id, [{RevPos-1, PrevRev}], []),
            PrevJson = couch_doc:to_json_obj(PrevDoc, []),
            case proc_prompt(Proc, [<<"st_index_doc">>, PrevJson]) of
                [[]] ->
                    ok;
                [PrevFeatures] ->
                    [ok = hastings_index:delete(IndexPid, Id, PrevVal)
                            || [PrevVal | _] <- PrevFeatures]
            end,
            if Del == true -> ok; true ->
                {ok, Doc} = couch_db:open_doc(Db, DI, []),
                Json = couch_doc:to_json_obj(Doc, []),
                case proc_prompt(Proc, [<<"st_index_doc">>, Json]) of
                    [[]] ->
                        ok;
                    [Features] ->
                        [ok = hastings_index:update(IndexPid, Id, Val)
                                || [Val | _] <- Features]
                end
            end
    end,
    {ok, setelement(1, Acc, I + 1)}.
