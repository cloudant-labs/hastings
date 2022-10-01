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

-module(hastings_fabric_search).


-export([
    go/4
]).


-include_lib("mem3/include/mem3.hrl").
-include_lib("couch/include/couch_db.hrl").
-include("hastings.hrl").


go(DbName, GroupId, IndexName, HQArgs) when is_binary(GroupId) ->
    {ok, DDoc} = fabric:open_doc(DbName, <<"_design/", GroupId/binary>>, []),
    go(DbName, DDoc, IndexName, HQArgs);

go(DbName, DDoc, IndexName, HQArgs) ->
    StartFun = search,
    StartArgs = [fabric_util:doc_id_and_rev(DDoc), IndexName, HQArgs],
    case run(DbName, StartFun, StartArgs, HQArgs) of
        {ok, Resps} ->
            Hits0 = merge_resps(Resps, HQArgs),
            Hits1 = limit_resps(Hits0, HQArgs),
            {ok, maybe_add_docs(DbName, Hits1, HQArgs)};
        Else ->
            Else
    end.


run(DbName, StartFun, StartArgs, #h_args{}=HQArgs) ->
    Primary = [
        {Node, Range} || {{Node, Range}, _} <- HQArgs#h_args.bookmark
    ],
    Stale = lists:member(HQArgs#h_args.stale, [true, update_after]),
    Stable = HQArgs#h_args.stable,
    Secondary = case Stale orelse Stable of
        true -> mem3:ushards(DbName);
        false -> []
    end,
    hastings_fabric:run(DbName, StartFun, StartArgs, Primary, Secondary).


merge_resps(Hits, #h_args{}=HQArgs) ->
    Limit = HQArgs#h_args.limit + HQArgs#h_args.skip,
    merge_resps(Hits, Limit);
merge_resps([{ok, Hits}], _Limit) ->
    Hits;
merge_resps([{ok, Hits} | Rest], Limit) ->
    RestHits = merge_resps(Rest, Limit),
    merge_hits(Hits, RestHits, Limit).


merge_hits(Hits, RestHits, Limit) ->
    SortFun = fun(H1, H2) ->
        {H1#h_hit.dist, H1#h_hit.id} =< {H2#h_hit.dist, H2#h_hit.id}
    end,
    SortedHits = lists:sort(SortFun, Hits ++ RestHits),
    lists:sublist(SortedHits, Limit).


limit_resps(Hits, HQArgs) ->
    limit_resps(Hits, HQArgs#h_args.skip, HQArgs#h_args.limit).


limit_resps(Hits, Skip, _Limit) when Skip >= length(Hits) ->
    [];
limit_resps(Hits, Skip, Limit) ->
    lists:sublist(Hits, Skip+1, Limit).


maybe_add_docs(DbName, Hits, #h_args{include_docs=true}) ->
    add_docs(DbName, Hits);
maybe_add_docs(_DbName, Hits, _) ->
    Hits.


add_docs(DbName, Hits) ->
    DocIds = [Id || #h_hit{id=Id} <- Hits],
    {ok, Docs} = hastings_util:get_json_docs(DbName, DocIds),
    lists:map(fun(H) ->
        {_, Doc} = lists:keyfind(H#h_hit.id, 1, Docs),
        H#h_hit{doc = Doc}
    end, Hits).
