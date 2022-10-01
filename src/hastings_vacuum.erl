% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(hastings_vacuum).


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
    clean_db/1,
    clean_db/2,
    clean_shard_db/1,
    clean_shard_db/2
]).


-record(st, {
    selective_clean = queue:new(),
    full_clean = queue:new(),
    selective_cleaner,
    full_cleaner
}).


-include_lib("mem3/include/mem3.hrl").
-include_lib("couch/include/couch_db.hrl").
-include("hastings.hrl").


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


cleanup(DbName) ->
    lists:foreach(fun(Node) ->
        rexi:cast(Node, {gen_server, cast, [?MODULE, {cleanup, DbName, []}]})
    end, mem3:nodes()).


init(_) ->
    couch_event:link_listener(?MODULE, handle_db_event, nil, [all_dbs]),
    {ok, #st{}}.


terminate(_Reason, _St) ->
    ok.


handle_call(Msg, _From, St) ->
    {stop, {bad_call, Msg}, {bad_call, Msg}, St}.


handle_cast({cleanup, ShardDbName, Options}, St) ->
    DbName = mem3:dbname(ShardDbName),
    Context = couch_util:get_value(context, Options, compaction),
    case Context =:= delete of
        true ->
            case queue:member(ShardDbName, St#st.full_clean) of
                true ->
                    {noreply, St};
                false ->
                    NewQ = queue:in(ShardDbName, St#st.full_clean),
                    maybe_start_full_cleaner(St#st{full_clean = NewQ})
            end;
        false ->
            case queue:member(DbName, St#st.selective_clean) of
                true ->
                    {noreply, St};
                false ->
                    NewQ = queue:in(DbName, St#st.selective_clean),
                    maybe_start_selective_cleaner(
                        St#st{selective_clean = NewQ})
            end
     end;

handle_cast(Msg, St) ->
    {stop, {bad_cast, Msg}, St}.


handle_info({'DOWN', _, _, Pid, _}, #st{selective_cleaner = Pid} = St) ->
    maybe_start_selective_cleaner(St#st{selective_cleaner = undefined});

handle_info({'DOWN', _, _, Pid, _}, #st{full_cleaner = Pid} = St) ->
    maybe_start_full_cleaner(St#st{full_cleaner = undefined});

handle_info(Msg, St) ->
    {stop, {bad_info, Msg}, St}.


code_change(_OldVsn, St, _Extra) ->
    {ok, St}.


handle_db_event(DbName, created, _St) ->
    gen_server:cast(?MODULE, {cleanup, DbName, []}),
    {ok, nil};
handle_db_event(DbName, deleted, _St) ->
    gen_server:cast(?MODULE, {cleanup, DbName, [{context, delete}]}),
    {ok, nil};
handle_db_event(_DbName, _Event, _St) ->
    {ok, nil}.


maybe_start_selective_cleaner(#st{selective_cleaner=undefined}=St) ->
    case queue:is_empty(St#st.selective_clean) of
        false ->
            start_selective_cleaner(St);
        true ->
            {noreply, St}
    end;

maybe_start_selective_cleaner(St) ->
    {noreply, St}.


maybe_start_full_cleaner(#st{full_cleaner=undefined}=St) ->
    case queue:is_empty(St#st.full_clean) of
        false ->
            start_full_cleaner(St);
        true ->
            {noreply, St}
    end;

maybe_start_full_cleaner(St) ->
    {noreply, St}.


start_selective_cleaner(St) ->
    {{value, DbName}, NewQ} = queue:out(St#st.selective_clean),
    {Pid, _} = spawn_monitor(?MODULE, clean_db, [DbName, []]),
    {noreply, St#st{selective_clean = NewQ, selective_cleaner = Pid}}.


start_full_cleaner(St) ->
    {{value, ShardDbName}, NewQ} = queue:out(St#st.full_clean),
    {Pid, _} = spawn_monitor(
        ?MODULE, clean_shard_db, [ShardDbName, [{context, delete}]]
    ),
    {noreply, St#st{full_clean = NewQ, full_cleaner = Pid}}.


clean_db(DbName) ->
    clean_db(DbName, []).

clean_db(DbName, _Options) ->
    delete_inactive_indexes(DbName, get_active_sigs(DbName)).


clean_shard_db(ShardDbName) ->
    clean_db(ShardDbName, []).

clean_shard_db(ShardDbName, Options) ->
    delete = couch_util:get_value(context, Options, delete),
    DoRecovery = config:get_boolean("couchdb",
        "enable_database_recovery", false),
    case DoRecovery of
        true ->
            rename_all_indexes(ShardDbName);
        false ->
            delete_all_indexes(ShardDbName)
    end.


get_active_sigs(DbName) ->
    {ok, JsonDDocs} = get_ddocs(DbName),
    DDocs = [couch_doc:from_json_obj(DD) || DD <- JsonDDocs],
    lists:usort(lists:flatmap(fun active_sigs/1, DDocs)).


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


delete_inactive_indexes(DbName, ActiveSigs) ->
    BaseDir = config:get("couchdb", "geo_index_dir", "/srv/geo_index"),
    DirList = hastings_util:get_existing_index_dirs(BaseDir, DbName),

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
            file:del_dir(IdxDir),
            cleanup_local_purge_doc(DbName, IdxDir)
        catch E:T:Stack ->
            couch_log:error("Failed to remove hastings index directory: ~p ~p",
                [{E, T}, Stack])
        end
    end, DeadDirs).


delete_all_indexes(ShardDbName) ->
    BaseDir = config:get("couchdb", "geo_index_dir", "/srv/geo_index"),
    DirList = hastings_util:get_existing_index_dirs(BaseDir, ShardDbName),

    lists:foreach(fun(IdxDir) ->
        try
            hastings_index:destroy(IdxDir),
            file:del_dir(IdxDir)
        catch E:T:Stack ->
            couch_log:error(
                "Failed to remove hastings index directory: ~p ~p",
                [{E, T}, Stack])
        end
    end, DirList).


rename_all_indexes(ShardDbName) ->
    BaseDir = config:get("couchdb", "geo_index_dir", "/srv/geo_index"),
    DirList = hastings_util:get_existing_index_dirs(BaseDir, ShardDbName),

    lists:foreach(fun(IdxDir) ->
        try
            hastings_util:do_rename(IdxDir)
        catch E:T:Stack ->
            couch_log:error(
                "Failed to rename hastings index directory: ~p ~p",
                [{E, T}, Stack])
        end
    end, DirList).


cleanup_local_purge_doc(DbName, IdxDir) ->
    Sig = hastings_util:get_signature_from_idxdir(IdxDir),
    case Sig of undefined -> ok; _ ->
        DocId = hastings_util:get_local_purge_doc_id(Sig),
        LocalShards = mem3:local_shards(DbName),
        lists:foldl(fun(LS, _AccOuter) ->
            ShardDbName = LS#shard.name,
            {ok, ShardDb} = couch_db:open_int(ShardDbName, []),
            case couch_db:open_doc(ShardDb, DocId, []) of
                {ok, LocalPurgeDoc} ->
                    couch_db:update_doc(ShardDb,
                        LocalPurgeDoc#doc{deleted=true}, [?ADMIN_CTX]);
                {not_found, _} ->
                    ok
            end,
            couch_db:close(ShardDb)
        end, [], LocalShards)
    end.
