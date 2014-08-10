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
    gen_server:cast(?MODULE, {cleanup, DbName}).


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
    {ok, JsonDDocs} = fabric:design_docs(DbName),
    DDocs = [couch_doc:from_json_obj(DD) || DD <- JsonDDocs],
    ActiveSigs = lists:usort(lists:flatmap(fun active_sigs/1, DDocs)),
    cleanup(DbName, ActiveSigs).


active_sigs(#doc{body={Fields}}=Doc) ->
    {RawIndexes} = couch_util:get_value(<<"indexes">>, Fields, {[]}),
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

    % Generate {Sig, IdxDir} mappings
    Pattern0 = filename:join([BaseDir, "shards", "*", DbName, "*"]),
    Pattern = binary_to_list(iolist_to_binary(Pattern0)),
    DirList = filelib:wildcard(Pattern),
    SigDirs = [{filename:rootname(D), D} || D <- DirList],

    % Remove any active sigs
    DeadSigDirs = lists:foldl(fun(Sig, Acc) ->
        lists:keydelete(Sig, 1, Acc)
    end, SigDirs, ActiveSigs),

    % Destroy anything that remains
    lists:foreach(fun({_Sig, IdxDir}) ->
        hastings_index:destroy(IdxDir)
    end, DeadSigDirs).
