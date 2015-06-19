%% Copyright 2014 Cloudant

-module(hastings_vacuum).


-include_lib("mem3/include/mem3.hrl").
-include_lib("couch/include/couch_db.hrl").
-include("hastings.hrl").


-export([
    start_link/0,
    cleanup/1
]).

-export([
    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3
]).

-export([
    handle_db_event/3,
    clean_db/1
]).


-record(st, {
    queue = queue:new(),
    cleaner
}).


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


cleanup(DbName) ->
    lists:foreach(fun(Node) ->
        rexi:cast(Node, {gen_server, cast, [?MODULE, {cleanup, DbName}]})
    end, mem3:nodes()).


init(_) ->
    couch_event:link_listener(?MODULE, handle_db_event, nil, [all_dbs]),
    {ok, #st{}}.


terminate(_Reason, _St) ->
    ok.


handle_call(Msg, _From, St) ->
    {stop, {bad_call, Msg}, {bad_call, Msg}, St}.


handle_cast({cleanup, DbName0}, St) ->
    DbName = mem3:dbname(DbName0),
    case queue:member(DbName, St#st.queue) of
        true ->
            {noreply, St};
        false ->
            NewQ = queue:in(DbName, St#st.queue),
            maybe_start_cleaner(St#st{queue = NewQ})
    end;

handle_cast(Msg, St) ->
    {stop, {bad_cast, Msg}, St}.


handle_info({'DOWN', _, _, Pid, _}, #st{cleaner = Pid} = St) ->
    maybe_start_cleaner(St#st{cleaner = undefined});

handle_info(Msg, St) ->
    {stop, {bad_info, Msg}, St}.


code_change(_OldVsn, St, _Extra) ->
    {ok, St}.


handle_db_event(DbName, created, _St) ->
    gen_server:cast(?MODULE, {cleanup, DbName}),
    {ok, nil};
handle_db_event(DbName, deleted, _St) ->
    gen_server:cast(?MODULE, {cleanup, DbName}),
    {ok, nil};
handle_db_event(_DbName, _Event, _St) ->
    {ok, nil}.


maybe_start_cleaner(#st{cleaner=undefined}=St) ->
    case queue:is_empty(St#st.queue) of
        false ->
            start_cleaner(St);
        true ->
            {noreply, St}
    end;

maybe_start_cleaner(St) ->
    {noreply, St}.


start_cleaner(St) ->
    {{value, DbName}, NewQ} = queue:out(St#st.queue),
    {Pid, _} = spawn_monitor(?MODULE, clean_db, [DbName]),
    {noreply, St#st{queue = NewQ, cleaner = Pid}}.


clean_db(DbName) ->
    {ok, JsonDDocs} = get_ddocs(DbName),
    DDocs = [couch_doc:from_json_obj(DD) || DD <- JsonDDocs],
    ActiveSigs = lists:usort(lists:flatmap(fun active_sigs/1, DDocs)),
    cleanup(DbName, ActiveSigs).


get_ddocs(DbName) ->
    {_, Ref} = spawn_monitor(fun() ->
        try fabric:design_docs(DbName) of
            {ok, DDocs} ->
                exit({ok, DDocs})
        catch
            throw:Reason ->
                exit({throw, Reason});
            error:Reason ->
                exit({error, Reason});
            exit:Reason ->
                exit({exit, Reason})
        end
    end),
    receive
        {'DOWN', Ref, _, _, {ok, DDocs}} ->
            {ok, DDocs};
        {'DOWN', Ref, _, _, {throw, Reason}} ->
            throw(Reason);
        {'DOWN', Ref, _, _, {error, database_does_not_exist}} ->
            exit(normal);
        {'DOWN', Ref, _, _, {error, Reason}} ->
            erlang:error(Reason);
        {'DOWN', Ref, _, _, {exit, Reason}} ->
            erlang:exit(Reason)
    end.


active_sigs(#doc{body={Fields}}=Doc) ->
    {RawIndexes} = couch_util:get_value(<<"st_indexes">>, Fields, {[]}),
    {IndexNames, _} = lists:unzip(RawIndexes),
    lists:flatmap(fun(Name) ->
        try
            {ok, Idx} = hastings_index:design_doc_to_index(Doc, Name),
            [Idx#h_idx.sig]
        catch _:_ ->
            []
        end
    end, IndexNames).


cleanup(DbName, ActiveSigs) ->
    BaseDir = config:get("couchdb", "geo_index_dir", "/srv/geo_index"),

    % Find the existing index directories on disk
    DbNamePattern = <<DbName/binary, ".*">>,
    Pattern0 = filename:join([BaseDir, "shards", "*", DbNamePattern, "*"]),
    Pattern = binary_to_list(iolist_to_binary(Pattern0)),
    DirListStrs = filelib:wildcard(Pattern),
    DirList = [iolist_to_binary(DL) || DL <- DirListStrs],

    % Create the list of active index directories
    LocalShards = mem3:local_shards(DbName),
    ActiveDirs = lists:foldl(fun(LS, AccOuter) ->
        lists:foldl(fun(Sig, AccInner) ->
            DirName = filename:join([BaseDir, LS#shard.name, Sig]),
            [DirName | AccInner]
        end, AccOuter, ActiveSigs)
    end, [], LocalShards),

    DeadDirs = DirList -- ActiveDirs,

    % Destroy anything that remains
    lists:foreach(fun(IdxDir) ->
        try
            hastings_index:destroy(IdxDir),
            file:del_dir(IdxDir)
        catch E:T ->
            Stack = erlang:get_stacktrace(),
            twig:log(error, "Failed to remove hastings index directory: ~p ~p",
                [{E, T}, Stack])
        end
    end, DeadDirs).
