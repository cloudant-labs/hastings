%% Copyright 2014 Cloudant

-module(hastings_util).


-export([
    get_json_docs/2,
    get_oldest_purge_seq/1,
    get_idx_purge_seq/2,
    close_index/1,
    get_local_purge_doc_id/1,
    get_value_from_options/2,
    update_local_purge_doc/6,
    maybe_create_local_purge_doc/2,
    get_signature_from_idxdir/1
]).


-include_lib("couch/include/couch_db.hrl").
-include("hastings.hrl").


-define(TIMEOUT, 300000).


get_json_docs(DbName, DocIds) ->
    Opts = [
        {keys, DocIds},
        {include_docs, true}
    ],
    fabric:all_docs(DbName, fun callback/2, [], Opts).


callback({meta, _}, Acc) ->
    {ok, Acc};
callback({total_and_offset,_,_}, Acc) ->
    {ok, Acc};
callback({error, Reason}, _Acc) ->
    {error, Reason};
callback({row, {Props}}, Acc) ->
    callback({row, Props}, Acc);
callback({row, Props}, Acc) when is_list(Props) ->
    {id, Id} = lists:keyfind(id, 1, Props),
    {doc, Doc} = lists:keyfind(doc, 1, Props),
    {ok, [{Id, Doc} | Acc]};
callback(complete, Acc) ->
    {ok, lists:reverse(Acc)};
callback(timeout, _Acc) ->
    {error, timeout}.


get_oldest_purge_seq(DbName) ->
    {ok, Db} = couch_db:open_int(DbName, []),
    try
        {ok, DbOldestPSeq} = couch_db:get_oldest_purge_seq(Db),
        DbOldestPSeq
    after
        couch_db:close(Db)
    end.


get_idx_purge_seq(DbName, Pid) ->
    DbOldestPurgeSeq = get_oldest_purge_seq(DbName),
    if DbOldestPurgeSeq > 0 ->
        easton_index:get(Pid, purge_seq, DbOldestPurgeSeq - 1);
    true ->
        easton_index:get(Pid, purge_seq, 0)
    end.


close_index(Pid) ->
    easton_index:close(Pid),
    receive
        {'EXIT', _, _} -> ok
    after ?TIMEOUT ->
        throw({timeout, reset_index})
    end.


maybe_create_local_purge_doc(Db, Index) ->
    Sig = Index#h_idx.sig,
    case couch_db:open_doc(Db, get_local_purge_doc_id(Sig), []) of
        {not_found, _Reason} ->
            DefaultPurgeSeq = couch_db:get_purge_seq(Db),
            PurgeSeq = easton_index:get(Index#h_idx.pid, purge_seq, DefaultPurgeSeq),
            update_local_purge_doc(
                Db,
                Index#h_idx.dbname,
                Index#h_idx.ddoc_id,
                Index#h_idx.name,
                Sig,
                PurgeSeq
            );
        {ok, _LocalPurgeDoc} ->
            ok
    end.


update_local_purge_doc(Db, DbName, DDocId, IndexName, Sig, PurgeSeq) ->
    Doc = couch_doc:from_json_obj({[
        {<<"_id">>, list_to_binary(?LOCAL_DOC_PREFIX ++ "purge-geo-" ++ Sig)},
        {<<"purge_seq">>, PurgeSeq},
        {<<"timestamp_utc">>, list_to_binary(couch_util:utc_string())},
        {<<"verify_module">>, <<"hastings_index">>},
        {<<"verify_function">>, <<"verify_index_exists">>},
        {<<"verify_options">>, {[
            {<<"dbname">>, DbName},
            {<<"ddoc_id">>, DDocId},
            {<<"indexname">>, IndexName},
            {<<"signature">>, Sig}
        ]}},
        {<<"type">>, <<"geo">>}
    ]}),
    couch_db:update_doc(Db, Doc, []).


get_value_from_options(Key, Options) ->
    case couch_util:get_value(Key, Options) of
        undefined ->
            Reason = binary_to_list(Key) ++ " must exist in Options.",
            throw({bad_request, Reason});
        Value -> Value
    end.


get_local_purge_doc_id(Sig) ->
    list_to_binary(?LOCAL_DOC_PREFIX ++ "purge-geo-" ++ Sig).


get_signature_from_idxdir(IdxDir) ->
    IdxDirList = filename:split(IdxDir),
    [Sig] = lists:nthtail(length(IdxDirList)-1, IdxDirList),
    Sig.
