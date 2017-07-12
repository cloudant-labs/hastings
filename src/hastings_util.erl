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
    rename_dir/3,
    get_existing_index_dirs/2,
    get_recovery_index_dir/3
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


rename_dir(RootDir, Original, DbName) ->
    RecoveryIndexDir = get_recovery_index_dir(RootDir, Original, DbName),
    Now = calendar:local_time(),
    filelib:ensure_dir(RecoveryIndexDir),
    case file:rename(Original, RecoveryIndexDir) of
        ok -> file:change_time(RecoveryIndexDir, Now);
        Else -> Else
    end.


get_existing_index_dirs(BaseDir, DbName) ->
    % Find the existing index directories on disk
    DbNamePattern = <<DbName/binary, ".*">>,
    Pattern0 = filename:join([BaseDir, "shards", "*", DbNamePattern, "*"]),
    Pattern = binary_to_list(iolist_to_binary(Pattern0)),
    DirListStrs = filelib:wildcard(Pattern),
    [iolist_to_binary(DL) || DL <- DirListStrs].


get_recovery_index_dir(RootDir, Original, DbName) ->
    PrefLen = binary:longest_common_prefix(
        [list_to_binary(RootDir), Original]
    ),
    <<_Pref:PrefLen/binary, Other/binary>> = Original,
    % skip directory separator, such as "/" or "\"
    GeoIndexAbsPath = case Other of
        <<"/", Rest/binary>> -> Rest;
        <<"\\", Rest/binary>> -> Rest;
        _ -> Other
    end,
    filename:join([RootDir, ".recovery", GeoIndexAbsPath]).
