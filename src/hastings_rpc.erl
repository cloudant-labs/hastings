%% Copyright 2014 Cloudant

-module(hastings_rpc).


-include_lib("couch/include/couch_db.hrl").
-include_lib("mem3/include/mem3.hrl").
-include("hastings.hrl").


-export([
    search/4,
    info/3
]).


search(Shard, DDocInfo, IndexName, HQArgs0) ->
    erlang:put(io_priority, {interactive, Shard#shard.name}),
    DDoc = get_ddoc(Shard, DDocInfo),
    HQArgs = set_bookmark(Shard, HQArgs0),
    AwaitSeq = get_await_seq(Shard#shard.name, HQArgs),
    Pid = get_index_pid(Shard#shard.name, DDoc, IndexName),
    case hastings_index:await(Pid, AwaitSeq) of
        {ok, _} -> ok;
        Error -> reply(Error)
    end,
    case hastings_index:search(Pid, HQArgs) of
        {ok, Results0} ->
            Results = lists:map(fun(R) -> fmt_result(Shard, R) end, Results0),
            reply({ok, Results});
        Else ->
            reply(Else)
    end.


info(Shard, DDocInfo, IndexName) ->
    erlang:put(io_priority, {interactive, Shard#shard.name}),
    DDoc = get_ddoc(Shard, DDocInfo),
    Pid = get_index_pid(Shard#shard.name, DDoc, IndexName),
    reply(hastings_index:info(Pid)).


get_ddoc(Shard, {DDocId, Rev}) ->
    {ok, DDoc} = ddoc_cache:open_doc(Shard#shard.dbname, DDocId, Rev),
    DDoc;
get_ddoc(_Shard, DDoc) ->
    DDoc.


set_bookmark(#shard{node=N, range=R}, #h_args{bookmark=Bookmark} = HQArgs) ->
    case lists:keyfind({N, R}, 1, Bookmark) of
        {{N, R}, Value} ->
            HQArgs#h_args{bookmark = Value};
        _ ->
            HQArgs#h_args{bookmark = undefined}
    end.


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
            couch_log:warning("~s creating ~s", [?MODULE, DbName]),
            couch_server:create(DbName, Options);
        Else ->
            Else
    end.


fmt_result(Shard, {DocId, Dist}) ->
    #h_hit{
        id = DocId,
        dist = Dist,
        shard = {Shard#shard.node, Shard#shard.range}
    };
fmt_result(Shard, {DocId, Dist, Geom}) ->
    #h_hit{
        id = DocId,
        dist = Dist,
        geom = Geom,
        shard = {Shard#shard.node, Shard#shard.range}
    }.


reply(Msg) ->
    rexi:reply(Msg),
    exit(normal).
