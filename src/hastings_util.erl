%% Copyright 2014 Cloudant

-module(hastings_util).


-export([
    get_json_docs/2,
    get_oldest_purge_seq/1,
    get_idx_purge_seq/2,
    close_index/1
]).


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
