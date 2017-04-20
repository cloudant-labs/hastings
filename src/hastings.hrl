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

-define(DEFAULT_SRID, 4326).


-record(h_idx, {
    pid,

    dbname,
    ddoc_id,
    name,
    def,
    lang,

    type,
    dimensions,
    srid,

    update_seq = 0,

    sig = nil
}).


-record(h_args, {
    geom,
    nearest = false,
    filter,

    req_srid = default,
    resp_srid = default,

    vbox,
    t_start,
    t_end,

    limit = 25,
    skip = 0,
    stale = false,
    stable = false,
    include_docs = false,
    include_geoms = true,
    bookmark = [],

    format = hastings_format_view,

    extra = []
}).


-record(h_hit, {
    id,
    dist,
    geom,
    doc,
    shard
}).
