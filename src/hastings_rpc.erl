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

-module(hastings_rpc).


-include_lib("couch/include/couch_db.hrl").
-include_lib("mem3/include/mem3.hrl").
-include("hastings.hrl").


-export([
    search/4,
    search/5,
    info/3,
    info/4,
    reply/1
]).


search(Shard, DDocInfo, IndexName, HQArgs) ->
    search(Shard#shard.name, Shard#shard.range, DDocInfo, IndexName, HQArgs).


search(ShardName, ShardRange, DDocInfo, IndexName, HQArgs0) ->
    erlang:put(io_priority, {interactive, ShardName}),
    DDoc = get_ddoc(ShardName, DDocInfo),
    HQArgs = set_bookmark(ShardRange, HQArgs0),
    AwaitSeq = get_await_seq(ShardName, HQArgs),
    Pid = get_index_pid(ShardName, DDoc, IndexName),
    case hastings_index:await(Pid, AwaitSeq) of
        {ok, _} -> ok;
        Error -> hastings_rpc:reply(Error)
    end,
    case hastings_index:search(Pid, HQArgs) of
        {ok, Results0} ->
            MapFun = fun(Result) -> fmt_result(node(), ShardRange, Result) end,
            Results = lists:map(MapFun, Results0),
            hastings_rpc:reply({ok, Results});
        Else ->
            hastings_rpc:reply(Else)
    end.


info(Shard, DDocInfo, IndexName) ->
    info(Shard#shard.name, Shard#shard.range, DDocInfo, IndexName).


info(ShardName, _ShardRange, DDocInfo, IndexName) ->
    erlang:put(io_priority, {interactive, ShardName}),
    DDoc = get_ddoc(ShardName, DDocInfo),
    Pid = get_index_pid(ShardName, DDoc, IndexName),
    hastings_rpc:reply(hastings_index:info(Pid)).


get_ddoc(ShardName, {DDocId, Rev}) ->
    DbName = mem3:dbname(ShardName),
    {ok, DDoc} = ddoc_cache:open_doc(DbName, DDocId, Rev),
    DDoc;
get_ddoc(_Shard, DDoc) ->
    DDoc.


set_bookmark(ShardRange, #h_args{bookmark=Bookmark} = HQArgs) ->
    Node = node(),
    case lists:keyfind({Node, ShardRange}, 1, Bookmark) of
        {{Node, ShardRange}, Value} ->
            HQArgs#h_args{bookmark = Value};
        _ ->
            HQArgs#h_args{bookmark = undefined}
    end.


get_index_pid(DbName, DDoc, IndexName) ->
    Index = case hastings_index:design_doc_to_index(DDoc, IndexName) of
        {ok, Index0} -> Index0;
        Error1 -> hastings_rpc:reply(Error1)
    end,
    case hastings_index_manager:get_index(DbName, Index) of
        {ok, Pid} -> Pid;
        Error2 -> hastings_rpc:reply(Error2)
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


fmt_result(Node, Range, {DocId, Dist}) ->
    #h_hit{
        id = DocId,
        dist = Dist,
        shard = {Node, Range}
    };
fmt_result(Node, Range, {DocId, Dist, Geom}) ->
    #h_hit{
        id = DocId,
        dist = Dist,
        geom = Geom,
        shard = {Node, Range}
    }.


reply(Msg) ->
    rexi:reply(Msg),
    exit(normal).
