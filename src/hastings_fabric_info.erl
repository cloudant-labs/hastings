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

-module(hastings_fabric_info).


-include("hastings.hrl").
-include_lib("mem3/include/mem3.hrl").
-include_lib("couch/include/couch_db.hrl").


-export([go/3]).


go(DbName, DDocId, IndexName) when is_binary(DDocId) ->
    {ok, DDoc} = fabric:open_doc(DbName, <<"_design/", DDocId/binary>>, []),
    go(DbName, DDoc, IndexName);

go(DbName, DDoc, IndexName) ->
    StartFun = info,
    StartArgs = [fabric_util:doc_id_and_rev(DDoc), IndexName],
    case hastings_fabric:run(DbName, StartFun, StartArgs) of
        {ok, Resps} ->
            merge_resps(Resps);
        Else ->
            Else
    end.


merge_resps(Resps) ->
    Dict = lists:foldl(fun({ok, Info}, D0) ->
        lists:foldl(fun({K, V}, D1) ->
            orddict:append(K, V, D1)
        end, D0, Info)
    end, orddict:new(), Resps),
    FinalInfo = orddict:fold(fun
        (disk_size, X, Acc) ->
            [{disk_size, lists:sum(X)} | Acc];
        (data_size, X, Acc) ->
            [{data_size, lists:sum(X)} | Acc];
        (doc_count, X, Acc) ->
            [{doc_count, lists:sum(X)} | Acc];
        (_, _, Acc) ->
            Acc
    end, [], Dict),
    {ok, FinalInfo}.
