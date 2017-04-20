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

-module(hastings_bookmark).


-export([
    unpack/1,
    pack/1,
    update/2
]).


-include("hastings.hrl").


unpack(Bin) when is_binary(Bin) ->
    Triples = binary_to_term(couch_util:decodeBase64Url(Bin), [safe]),
    lists:map(fun({N, R, A}) -> {{N, R}, A} end, Triples).


pack(Bookmark) ->
    Triples = [{N, R, A} || {{N, R}, A} <- Bookmark],
    Bin = term_to_binary(Triples, [compressed, {minor_version,1}]),
    couch_util:encodeBase64Url(Bin).


% Its important to note that this function expects the
% list of hits to be passed sorted in ascending order
% of {Distance, DocId}.
update(Bookmark, []) ->
    Bookmark;
update(Bookmark, [Hit | Rest]) ->
    Key = Hit#h_hit.shard,
    Val = {Hit#h_hit.id, Hit#h_hit.dist},
    NewBookmark = lists:keystore(Key, 1, Bookmark, {Key, Val}),
    update(NewBookmark, Rest).
