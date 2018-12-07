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

-module(hastings_util).


-export([
    get_json_docs/2,
    do_rename/1,
    get_existing_index_dirs/2,
    calculate_delete_directory/1,
    get_oldest_purge_seq/1,
    get_idx_purge_seq/2,
    close_index/1,
    get_local_purge_doc_id/1,
    get_value_from_options/2,
    ensure_local_purge_docs/2,
    update_local_purge_doc/6,
    maybe_create_local_purge_doc/2,
    get_signature_from_idxdir/1,
    verify_index_exists/2
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


do_rename(IdxDir) ->
    IdxName = filename:basename(IdxDir),
    DbDir = filename:dirname(IdxDir),
    DeleteDir = calculate_delete_directory(DbDir),
    RenamePath = filename:join([DeleteDir, IdxName]),
    filelib:ensure_dir(RenamePath),
    Now = calendar:local_time(),
    case file:rename(IdxDir, RenamePath) of
        ok -> file:change_time(DeleteDir, Now);
        Else -> Else
    end.


get_existing_index_dirs(BaseDir, ShardDbName) ->
    Pattern0 = filename:join([BaseDir, ShardDbName, "*"]),
    Pattern = binary_to_list(iolist_to_binary(Pattern0)),
    DirListStrs = filelib:wildcard(Pattern),
    [iolist_to_binary(DL) || DL <- DirListStrs].


calculate_delete_directory(DbNameDir) ->
    {{Y, Mon, D}, {H, Min, S}} = calendar:universal_time(),
    Suffix = lists:flatten(
        io_lib:format(".~w~2.10.0B~2.10.0B."
        ++ "~2.10.0B~2.10.0B~2.10.0B.deleted"
        ++ filename:extension(binary_to_list(DbNameDir)),
            [Y, Mon, D, H, Min, S])
    ),
    binary_to_list(filename:rootname(DbNameDir)) ++ Suffix.


get_oldest_purge_seq(DbName) ->
    couch_util:with_db(DbName, fun(Db) ->
        couch_db:get_oldest_purge_seq(Db)
    end).


get_idx_purge_seq(DbName, Pid) ->
    OldestSeq = get_oldest_purge_seq(DbName),
    erlang:max(0, easton_index:get(Pid, purge_seq, OldestSeq - 1)).


close_index(Pid) ->
    easton_index:close(Pid),
    receive
        {'EXIT', _, _} -> ok
    after ?TIMEOUT ->
        throw({timeout, close_index})
    end.


ensure_local_purge_docs(DbName, DDocs) ->
    couch_util:with_db(DbName, fun(Db) ->
        lists:foreach(fun(DDoc) ->
            #doc{body = {Props}} = DDoc,
            case couch_util:get_value(<<"st_indexes">>, Props) of
                undefined ->
                    ok;
                _ ->
                    try hastings_index:design_doc_to_indexes(DDoc) of
                        STIndexes -> ensure_local_purge_doc(Db, STIndexes)
                    catch _:_ ->
                        ok
                    end
            end
         end, DDocs)
    end).


ensure_local_purge_doc(Db, STIndexes) ->
    if STIndexes == [] -> ok; true ->
        lists:map(fun(STIndex) ->
            maybe_create_local_purge_doc(Db, STIndex)
        end, STIndexes)
    end.


maybe_create_local_purge_doc(Db, Index) ->
    Sig = Index#h_idx.sig,
    case couch_db:open_doc(Db, get_local_purge_doc_id(Sig), []) of
        {not_found, _Reason} ->
            DefaultPurgeSeq = couch_db:get_purge_seq(Db),
            PurgeSeq = try easton_index:get(Index#h_idx.pid, purge_seq) of
                false -> DefaultPurgeSeq;
                IdxPurgeSeq -> IdxPurgeSeq
            catch _:_ ->
                DefaultPurgeSeq
            end,
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
    {Mega, Secs, _} = os:timestamp(),
    NowSecs = Mega * 1000000 + Secs,
    Doc = couch_doc:from_json_obj({[
        {<<"_id">>, get_local_purge_doc_id(Sig)},
        {<<"purge_seq">>, PurgeSeq},
        {<<"updated_on">>, NowSecs},
        {<<"ddoc_id">>, DDocId},
        {<<"indexname">>, IndexName},
        {<<"signature">>, Sig},
        {<<"type">>, <<"hastings">>}
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
    ?l2b(?LOCAL_DOC_PREFIX ++ "purge-" ++ "hastings-" ++ Sig).


get_signature_from_idxdir(IdxDir) ->
    IdxDirList = filename:split(IdxDir),
    Sig = lists:last(IdxDirList),
    case [Ch || Ch <- Sig, not (((Ch >= $0) and (Ch =< $9))
        orelse ((Ch >= $a) and (Ch =< $f))
        orelse ((Ch >= $A) and (Ch =< $F)))] == [] of
        true -> Sig;
        false -> undefined
    end.


verify_index_exists(DbName, Props) ->
    try
        Type = couch_util:get_value(<<"type">>, Props),
        if Type =/= <<"hastings">> -> false; true ->
            DDocId = couch_util:get_value(<<"ddoc_id">>, Props),
            IndexName = couch_util:get_value(<<"indexname">>, Props),
            Sig = couch_util:get_value(<<"signature">>, Props),
            couch_util:with_db(DbName, fun(Db) ->
                case couch_db:get_design_doc(Db, DDocId) of
                    {ok, #doc{} = DDoc} ->
                        {ok, IdxState} = hastings_index:design_doc_to_index(
                            DDoc, IndexName),
                        IdxState#h_idx.sig == Sig;
                    {not_found, _} ->
                        false
                end
            end)
        end
    catch _:_ ->
        false
    end.
