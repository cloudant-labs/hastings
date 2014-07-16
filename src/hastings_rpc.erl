%% Copyright 2014 Cloudant

-module(hastings_rpc).


-include_lib("couch/include/couch_db.hrl").
-include("hastings.hrl").


-export([
    search/4,
    info/3
]).

search(DbName, {DDocId, Rev}, IndexName, HQArgs) ->
    erlang:put(io_priority, {interactive, DbName}),
    {ok, DDoc} = ddoc_cache:open_doc(mem3:dbname(DbName), DDocId, Rev),
    search(DbName, DDoc, IndexName, HQArgs);
search(DbName, DDoc, IndexName, HQArgs) ->
    erlang:put(io_priority, {interactive, DbName}),
    AwaitSeq = get_await_seq(DbName, HQArgs),
    Pid = get_index_pid(DbName, DDoc, IndexName),
    case hastings_index:await(Pid, AwaitSeq) of
        ok -> ok;
        Error -> reply(Error)
    end,
    reply(hastings_index:search(Pid, HQArgs)).


info(DbName, {DDocId, Rev}, IndexName) ->
    erlang:put(io_priority, {interactive, DbName}),
    {ok, DDoc} = ddoc_cache:open_doc(mem3:dbname(DbName), DDocId, Rev),
    info(DbName, DDoc, IndexName);
info(DbName, DDoc, IndexName) ->
    erlang:put(io_priority, {interactive, DbName}),
    Pid = get_index_pid(DbName, DDoc, IndexName),
    reply(hastings_index:info(Pid)).


get_index_pid(DbName, DDoc, IndexName) ->
    Index = case hastings_index:design_doc_to_index(DDoc, IndexName) of
        {ok, Index0} -> Index0;
        Error1 -> reply(Error1)
    end,
    case hastings_index_manager:get_index(DbName, Index) of
        {ok, Pid} -> Pid;
        Error2 -> reply(Error2)
    end.


get_await_seq(DbName, HQArgs) ->
    case HQArgs#h_args.stale of
        true -> 0;
        update_after -> 0;
        false -> get_update_seq(DbName)
    end.


get_update_seq(DbName) ->
    {ok, Db} = get_or_create_db(DbName, []),
    try
        couch_db:get_update_seq(Db)
    after
        couch_db:close(Db)
    end.


get_or_create_db(DbName, Options) ->
    case couch_db:open_int(DbName, Options) of
        {not_found, no_db_file} ->
            twig:log(warn, "~s creating ~s", [?MODULE, DbName]),
            couch_server:create(DbName, Options);
        Else ->
            Else
    end.


reply(Msg) ->
    rexi:reply(Msg),
    exit(normal).
