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

-module(hastings_format_legacy).

-include("hastings.hrl").

-export([hits_to_json/3]).

hits_to_json(_DbName, Hits, HQArgs) ->
    Bookmark = hastings_bookmark:update(HQArgs#h_args.bookmark, Hits),
    BookmarkJson = hastings_bookmark:pack(Bookmark),
    {[], hits_to_json0(Hits, BookmarkJson)}.


hits_to_json0(Hits, Bookmark) ->
    Docs = lists:map(fun(H) ->
        case H#h_hit.doc of
            undefined ->
                {[{<<"id">>, H#h_hit.id}]};
            {Props0} ->
                Props = filter_props(Props0),
                Extra = case lists:keyfind(<<"type">>, 1, Props) of
                    false ->
                        [{<<"type">>, <<"Feature">>}];
                    _ ->
                        []
                end,
                {[{<<"id">>, H#h_hit.id}] ++ Extra ++ Props}
        end
    end, Hits),
    {[
        {<<"type">>, <<"FeatureCollection">>},
        {<<"features">>, Docs},
        {<<"bookmark">>, Bookmark}
    ]}.


filter_props(Props) ->
    Keys = [<<"_id">>, <<"_rev">>],
    lists:foldl(fun(Key, Acc) ->
        lists:keydelete(Key, 1, Acc)
    end, Props, Keys).

