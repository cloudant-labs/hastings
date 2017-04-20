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

-module (hastings_index_test).

-include_lib("eunit/include/eunit.hrl").

design_doc_to_index_int_test() ->
    meck:new(couch_util),
    meck:expect(couch_util, get_value, fun
        (_, [<<"p1">>]) -> {[]};
        (_, [<<"p2">>]) -> {[{<<"p2">>, <<"broken">>}]}
    end),
    ?assertThrow({not_found, <<"Geospatial index 'p1' not found">>},
        hastings_index:design_doc_to_index_int(id, [<<"p1">>], <<"p1">>)
    ),
    ?assertThrow({invalid_index, <<"Geospatial index 'p2' is invalid">>},
        hastings_index:design_doc_to_index_int(id, [<<"p2">>], <<"p2">>)
    ),
    meck:unload(couch_util).
