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

-module(hastings_plugin_couch_db).

-export([
    is_valid_purge_client/2,
    on_compact/2
]).


-include_lib("couch/include/couch_eunit.hrl").


is_valid_purge_client(DbName, Props) ->
    hastings_util:verify_index_exists(DbName, Props).


on_compact(DbName, DDocs) ->
    hastings_util:ensure_local_purge_docs(DbName, DDocs).
