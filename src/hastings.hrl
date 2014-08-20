

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

    extra = []
}).


-record(h_hit, {
    id,
    dist,
    geom,
    doc,
    shard
}).
