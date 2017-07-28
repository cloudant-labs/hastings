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

-module(hastings_disk_size_tests).


-include_lib("couch/include/couch_eunit.hrl").
-include_lib("couch/include/couch_db.hrl").


-define(TIMEOUT, 10000).


setup() ->
    DbName = ?tempdb(),
    ok = fabric:create_db(DbName, [?ADMIN_CTX]),
    DbName.


teardown(DbName) ->
    ok = fabric:delete_db(DbName, [?ADMIN_CTX]).


disk_size_test_() ->
    {
        "hastings handle disk size test",
        {
            setup,
            fun() -> test_util:start_couch([fabric, mem3, hastings, easton]) end,
            fun test_util:stop/1,
            {
                foreach,
                fun setup/0, fun teardown/1,
                [
                    fun disk_size_test/1
                ]
            }
        }
    }.


disk_size_test(DbName) ->
    ?_test(begin
        Fn = filename:join([?APPDIR, "test", "testdata", "geo_docs2.json"]),
        {ok, Bin} = file:read_file(Fn),
        DocsArray = jiffy:decode(Bin),
        Docs = [couch_doc:from_json_obj(JsonObj) || JsonObj <- DocsArray],

        DesignFn = filename:join([?APPDIR, "test", "testdata",
            "geo_design.json"]),
        {ok, Bin2} = file:read_file(DesignFn),
        JsonDDoc = jiffy:decode(Bin2),
        DDoc = couch_doc:from_json_obj(JsonDDoc),

        AllDocs = [DDoc|Docs],
        {ok, _} = fabric:update_docs(DbName, AllDocs, [?ADMIN_CTX]),

        % check that disk_size query doesn't open any hastings_index processes
        IndexName = <<"geoidx">>,
        {ok, DDoc1} = fabric:open_doc(DbName, <<"_design/geodd">>, [?ADMIN_CTX]),
        hastings_fabric_info:go(DbName, DDoc1, IndexName, disk_size),
        Size = ets:info(hastings_by_pid, size),
        ?assertEqual(0, Size),

        % check that disk_size query response is in the proper format
        HQArgs = {h_args,{ circle, {-71.12087954, 42.35182135 ,100.0}}, false,
            <<"contains">>, default, default, undefined, undefined, undefined,
            25, 0, false, false, false, true, [], hastings_format_view, []},
        hastings_fabric_search:go(DbName, DDoc1, IndexName, HQArgs),
        Resp = hastings_fabric_info:go(DbName, DDoc1, IndexName, disk_size),
        ?assertMatch({ok,[{disk_size, X}]} when X>0, Resp),
        ok
    end).
