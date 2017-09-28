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

-module(hastings_delete_db_test).


-include_lib("couch/include/couch_eunit.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("hastings/src/hastings.hrl").


-define(TIMEOUT, 10000).


setup() ->
    DbName = ?tempdb(),
    ok = fabric:create_db(DbName, [?ADMIN_CTX]),
    meck:new(hastings_vacuum, [passthrough]),
    meck:expect(hastings_vacuum, clean_db, fun(A) ->
        meck:passthrough([A])
    end),
    DbName.


teardown(_DbName) ->
    (catch meck:unload(hastings_vacuum)).


hastings_delete_db_test_() ->
    {
        "Hastings Delete Database Tests",
        {
            setup,
            fun() -> test_util:start_couch([fabric, mem3, hastings]) end,
            fun test_util:stop/1,
            {
                foreach,
                fun setup/0, fun teardown/1,
                [
                    fun should_delete_index_after_deleting_database/1,
                    fun should_move_index_after_deleting_database/1
                ]
            }
        }
    }.


should_delete_index_after_deleting_database(DbName) ->
    ?_test(begin
        Docs = hastings_test_util:make_docs(5),
        {ok, _} = fabric:update_docs(DbName, Docs, [?ADMIN_CTX]),
        {ok, _} = fabric:update_doc(
          DbName,
          hastings_test_util:ddoc(geo),
          [?ADMIN_CTX]
        ),
        
        {ok, Hits} = run_hastings_search(DbName),
        ?assertEqual(5, lists:flatlength(Hits)),
        
        config:set("couchdb", "enable_database_recovery", "false", false),
        BaseDir = config:get("couchdb", "geo_index_dir", "/srv/geo_index"),
        
        ExistingDirs = hastings_util:get_existing_index_dirs(BaseDir, DbName),
        [GeoIndexDir | _Rest] = ExistingDirs,
        GeoDirExistsBefore = filelib:is_dir(GeoIndexDir),
        
        fabric:delete_db(DbName, [?ADMIN_CTX]),
        meck:wait(hastings_vacuum, clean_db, '_', 5000),
        
        RecDir = hastings_util:get_recovery_index_dir(
            BaseDir,
            GeoIndexDir,
            DbName
        ),
        GeoDirExistsAfter = filelib:is_dir(GeoIndexDir),
        GeoRecDirExistsAfter = filelib:is_dir(RecDir),
        
        [
            ?assert(GeoDirExistsBefore),
            ?assertNot(GeoDirExistsAfter),
            ?assertNot(GeoRecDirExistsAfter)
        ]
    end).


should_move_index_after_deleting_database(DbName) ->
    ?_test(begin
        Docs = hastings_test_util:make_docs(5),
        {ok, _} = fabric:update_docs(DbName, Docs, [?ADMIN_CTX]),
        {ok, _} = fabric:update_doc(
            DbName,
            hastings_test_util:ddoc(geo),
            [?ADMIN_CTX]
        ),
    
        {ok, Hits} = run_hastings_search(DbName),
        ?assertEqual(5, lists:flatlength(Hits)),
    
        config:set("couchdb", "enable_database_recovery", "true", false),
        BaseDir = config:get("couchdb", "geo_index_dir", "/srv/geo_index"),
    
        ExistingDirs = hastings_util:get_existing_index_dirs(BaseDir, DbName),
        [GeoIndexDir | _Rest] = ExistingDirs,
        GeoDirExistsBefore = filelib:is_dir(GeoIndexDir),
    
        fabric:delete_db(DbName, [?ADMIN_CTX]),
        meck:wait(hastings_vacuum, clean_db, '_', 5000),
    
        RecDir = hastings_util:get_recovery_index_dir(
            BaseDir,
            GeoIndexDir,
            DbName
        ),
        GeoDirExistsAfter = filelib:is_dir(GeoIndexDir),
        GeoRecDirExistsAfter = filelib:is_dir(RecDir),
    
        [
            ?assert(GeoDirExistsBefore),
            ?assertNot(GeoDirExistsAfter),
            ?assert(GeoRecDirExistsAfter)
        ]
    end).


run_hastings_search(DbName) ->
    HQArgs = #h_args{geom = {bbox,[-180.0,-90.0,180.0,90.0]}},
    {ok, DDoc} = fabric:open_doc(DbName, <<"_design/geodd">>, []),
    hastings_fabric_search:go(DbName, DDoc, <<"geoidx">>, HQArgs).
