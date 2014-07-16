

-define(DEFAULT_SRID, 4326).


-record(h_idx, {
    pid,

    dbname,
    ddoc_id,
    name,
    def,
    lang,

    type,
    dimension,
    srid,

    update_seq = 0,

    sig = nil
}).


-record(h_args, {
    geom,
    nearest = false,
    filter,

    req_srid = 0,
    resp_srid = 0,

    start_time,
    end_time,

    limit = 25,
    skip = 0,
    stale = false,
    include_docs = false
}).


-record(h_acc, {
    counters,
    resps
}).


-record(h_hit, {
    id,
    geom,
    doc
}).
