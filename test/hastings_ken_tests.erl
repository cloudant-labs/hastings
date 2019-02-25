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

-module(hastings_ken_tests).


-include_lib("couch/include/couch_eunit.hrl").
-include_lib("couch/include/couch_db.hrl").


-define(TIMEOUT, 10000).


setup() ->
    DbName = ?tempdb(),
    ok = fabric:create_db(DbName, [?ADMIN_CTX]),
    meck:new(hastings_index,[passthrough]),
    meck:expect(hastings_index, design_doc_to_indexes, fun(_) -> [] end),
    DbName.


teardown(DbName) ->
    meck:unload(hastings_index),
    ok = fabric:delete_db(DbName, [?ADMIN_CTX]).


ken_test() ->
    {
        "Ken Tests",
        {
            setup,
            fun() -> test_util:start_couch([ken, fabric, mem3, hastings]) end,
            fun test_util:stop/1,
            {
                foreach,
                fun setup/0, fun teardown/1,
                [
                    fun ken/1
                ]
            }
        }
    }.


ken(DbName) ->
    ?_test(begin
        Fn = filename:join([?APPDIR, "test", "testdata", "geo_docs.json"]),
        {ok, Bin} = file:read_file(Fn),
        DocsArray = jiffy:decode(Bin),
        Docs = [couch_doc:from_json_obj(JsonObj) || JsonObj <- DocsArray],

        DesignFn = filename:join([?APPDIR, "test", "testdata",
            "geo_design.json"]),
        {ok, Bin2} = file:read_file(DesignFn),
        JsonDDoc = jiffy:decode(Bin2),
        DDoc = couch_doc:from_json_obj(JsonDDoc),

        {ok, _} = fabric:update_docs(DbName, Docs, [?ADMIN_CTX]),

        Pid = ets:first(hastings_by_pid),
        ?assertEqual(false, is_pid(Pid)),

        ok = meck:reset(hastings_index),
        {ok, _} = fabric:update_doc(DbName, DDoc, [?ADMIN_CTX]),
        ?assertEqual(ok, meck:wait(8, hastings_index,design_doc_to_indexes, '_', 15000)),
        ok
    end).
