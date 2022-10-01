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

-module(hastings_format_view).

-export([hits_to_json/3]).

-include("hastings.hrl").

hits_to_json(DbName, Hits, HQArgs) ->
    Bookmark = hastings_bookmark:update(HQArgs#h_args.bookmark, Hits),
    BookmarkJson = hastings_bookmark:pack(Bookmark),
    {[], hits_to_json0(DbName, Hits, HQArgs, BookmarkJson)}.


hits_to_json0(DbName, Hits, HQArgs, Bookmark) ->
    Hits0 = maybe_add_docs_in_view_format(DbName, Hits, HQArgs),
    Docs = lists:map(fun(H) ->
        Geom = case H#h_hit.geom of
            undefined -> null;
            Geom0 -> Geom0
        end,
        case HQArgs#h_args.include_docs of
            false ->
                {_, Rev} = lists:keyfind(<<"_rev">>, 1,H#h_hit.doc),
                {[
                    {<<"id">>, H#h_hit.id},
                    {<<"rev">>, Rev},
                    {<<"geometry">>, Geom}
                ]};
            true ->
                {[
                    {<<"id">>, H#h_hit.id},
                    {<<"geometry">>, Geom},
                    {doc, H#h_hit.doc}
                ]}
        end
    end, Hits0),
    {[
        {<<"bookmark">>, Bookmark},
        {<<"rows">>, Docs}
    ]}.

maybe_add_docs_in_view_format(DbName, Hits, #h_args{include_docs=false}) ->
    add_docs_in_view_format(DbName, Hits);
maybe_add_docs_in_view_format(_DbName, Hits, _) ->
    Hits.


add_docs_in_view_format(DbName, Hits) ->
    DocIds = [Id || #h_hit{id=Id} <- Hits],
    {ok, Docs} = hastings_util:get_json_docs(DbName, DocIds),
    lists:map(fun(H) ->
        {_, {Doc}} = lists:keyfind(H#h_hit.id, 1, Docs),
        H#h_hit{doc = Doc}
    end, Hits).
