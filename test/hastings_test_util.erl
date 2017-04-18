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

-module(hastings_test_util).

-compile(export_all).

-include_lib("couch/include/couch_db.hrl").
-include_lib("couch/include/couch_eunit.hrl").


save_docs(DbName, Docs) ->
    {ok, _} = fabric:update_docs(DbName, Docs, [?ADMIN_CTX]).


make_docs(Count) ->
    [doc(I) || I <- lists:seq(1, Count)].


ddoc(geo) ->
    couch_doc:from_json_obj({[
        {<<"_id">>, <<"_design/geodd">>},
        {<<"st_indexes">>, {[
            {<<"geoidx">>, {[
                {<<"index">>, <<
                    "function(doc) {"
                    "  if (doc.geometry && doc.geometry.coordinates)"
                    "    {st_index(doc.geometry);}"
                    "}"
                >>}
            ]}}
        ]}}
    ]}).


doc(Id) ->
    couch_doc:from_json_obj({[
        {<<"_id">>, list_to_binary("point" ++ integer_to_list(Id))},
        {<<"val">>, Id},
        {<<"geometry">>, {[
            {<<"type">>, <<"Point">>},
            {<<"coordinates">>, [Id, Id]}
        ]}}
    ]}).
