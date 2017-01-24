%% Copyright 2014 Cloudant

-module(hastings_util).


-export([
    get_json_docs/2,
    get_oldest_purge_seq/1,
    get_idx_purge_seq/2,
    close_index/1,
    get_local_purge_doc_id/1,
    get_value_from_options/2,
    create_or_update_local_purge_doc/6,
    utc_string/0
]).


-include_lib("couch/include/couch_db.hrl").


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


create_or_update_local_purge_doc(Db, DbName, DDocId, IndexName, Sig, PSeq) ->
    case couch_db:open_doc(Db, get_local_purge_doc_id(Sig), []) of
        {not_found, _Reason} ->
            DocContent = get_local_purge_doc_content(DbName, DDocId, IndexName, Sig, PSeq, hastings_util:utc_string());
        {ok, LocalPurgeDoc} ->
            Body = get_local_purge_doc_body(DbName, DDocId, IndexName, PSeq, hastings_util:utc_string()),
            DocContent = LocalPurgeDoc#doc{body=Body}
    end,
    couch_db:update_doc(Db, DocContent, []).


get_local_purge_doc_content(DbName, DDocId, IndexName, Sig, PurgeSeq, LastUpdateTs) ->
    couch_doc:from_json_obj({[
        {<<"_id">>, list_to_binary(?LOCAL_DOC_PREFIX ++ "purge-geo-" ++ Sig)},
        {<<"purge_seq">>, PurgeSeq},
        {<<"timestamp_utc">>, list_to_binary(LastUpdateTs)},
        {<<"verify_module">>, <<"hastings_index">>},
        {<<"verify_function">>, <<"verify_index_exists">>},
        {<<"verify_options">>, {[
            {<<"dbname">>, DbName},
            {<<"ddoc_id">>, DDocId},
            {<<"indexname">>, IndexName}
        ]}},
        {<<"type">>, <<"geo">>}
    ]}).


get_local_purge_doc_body(DbName, DDocId, IndexName, PurgeSeq, LastUpdateTs) ->
    {[
        {<<"purge_seq">>, PurgeSeq},
        {<<"timestamp_utc">>, list_to_binary(LastUpdateTs)},
        {<<"verify_module">>, <<"hastings_index">>},
        {<<"verify_function">>, <<"verify_index_exists">>},
        {<<"verify_options">>, {[
            {<<"dbname">>, DbName},
            {<<"ddoc_id">>, DDocId},
            {<<"indexname">>, IndexName}
        ]}},
        {<<"type">>, <<"geo">>}
    ]}.


get_value_from_options(Key, Options) ->
    case couch_util:get_value(Key, Options) of
        undefined ->
            Reason = binary_to_list(Key) ++ " must exist in Options.",
            throw({bad_request, Reason});
        Value -> Value
    end.


utc_string() ->
    {MegaSecs, Secs, MicroSecs} = erlang:now(),
    {{Year, Month, Day}, {Hour, Minute, Second}} =
        calendar:now_to_universal_time({MegaSecs, Secs, MicroSecs}),
    lists:flatten(
        io_lib:format("~4..0w-~2..0w-~2..0wT~2..0w:~2..0w:~2..0w.~6..0wZ",
            [Year, Month, Day, Hour, Minute, Second, MicroSecs])).


get_local_purge_doc_id(Sig) ->
    list_to_binary(?LOCAL_DOC_PREFIX ++ "purge-geo-" ++ Sig).
