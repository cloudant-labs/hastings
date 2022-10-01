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

-module(hastings_format_geojson).
-behaviour(hastings_format).

-export([hits_to_json/3]).

-include("hastings.hrl").

hits_to_json(_DbName, Hits, HQArgs) ->
    Bookmark = hastings_bookmark:update(HQArgs#h_args.bookmark, Hits),
    BookmarkJson = hastings_bookmark:pack(Bookmark),
    {[], hits_to_json0(Hits, BookmarkJson)}.

hits_to_json0(Hits, Bookmark) ->
    Geoms = lists:map(fun(H) ->
        Geom = case H#h_hit.geom of
            undefined -> null;
            Geom0 -> Geom0
        end,
        ExtraProps = case H#h_hit.doc of
            undefined -> [{<<"type">>, <<"Feature">>}, {<<"properties">>, []}];
            {DocList0} ->
                Props0 = case lists:keyfind(<<"properties">>, 1, DocList0) of
                    {_, Props} -> Props;
                _ -> []
                end,
                Rev0 = case lists:keyfind(<<"_rev">>, 1, DocList0) of
                    {_, Rev} -> Rev;
                    _ -> null
                end,
                Type0 = case lists:keyfind(<<"type">>, 1, DocList0) of
                    {_, Type} -> Type;
                    _ -> <<"Feature">>
                end,
                [
                    {<<"_rev">>, Rev0},
                    {<<"type">>, Type0},
                    {<<"properties">>, Props0}
                ]
        end,
        {[
            {<<"_id">>, H#h_hit.id},
            {<<"geometry">>, Geom}
            | ExtraProps
        ]}
    end, Hits),
    {[
        {<<"bookmark">>, Bookmark},
        {<<"type">>, <<"FeatureCollection">>},
        {<<"features">>, Geoms}
    ]}.