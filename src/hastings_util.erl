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

-include_lib("couch/include/couch_eunit.hrl").
-module(hastings_util).


-export([
    get_json_docs/2,
    do_rename/1,
    get_existing_index_dirs/2,
    calculate_delete_directory/1
]).


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
    ?debugFmt("Renaming from ~n~p~n to ~n~p~n", [IdxDir, RenamePath]),
    case file:rename(IdxDir, RenamePath) of
        ok ->
            ?debugMsg("Success..."),
            file:change_time(DeleteDir, Now);
        Else ->
            ?debugFmt("Failed with ~n~p~n...", [Else]),
            Else
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
