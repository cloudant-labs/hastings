%% Copyright 2014 Cloudant

-module(hastings_rpc).


-include_lib("couch/include/couch_db.hrl").
-include("hastings.hrl").


-export([
    search/4,
    info/3
]).


search(DbName, DDoc, IndexName, HQArgs) ->
    erlang:put(io_priority, {interactive, DbName}),
    AwaitSeq = get_await_seq(DbName, HQArgs),
    Pid = get_index_pid(DbName, DDoc, IndexName),
    case hastings_index:await(Pid, AwaitSeq) of
        ok -> ok;
        Error -> reply(Error)
    end,
    reply(hastings_index:search(Pid, QueryArgs)).


info(DbName, DDoc, IndexName) ->
    erlang:put(io_priority, {interactive, DbName}),
    Pid = get_index_pid(DbName, DDoc, IndexName),
    reply(hastings_index:info(Pid)).


get_index_pid(DbName, DDoc, IndexName) ->
    Index = case hastings_index:design_doc_to_index(DDoc, IndexName) of
        {ok, Index0} -> Index0;
        Error -> reply(Error)
    end,
    case hastings_index_manager:get_index(DbName, Index) of
        {ok, Pid} -> Pid;
        Error -> reply(Error)
    end.


get_await_seq(DbName, HQArgs) ->
    case HQArgs#hq_args.stale of
        true -> 0;
        update_after -> 0;
        false -> get_update_seq(DbName)
    end.


get_update_seq(DbName) ->
    {ok, Db} = get_or_create_db(Db, []),
    try
        LastSeq = couch_db:get_update_seq(Db)
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
