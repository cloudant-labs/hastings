
% CRS
-define(WGS84_LL, "urn:ogc:def:crs:EPSG::4326").


-record(index, {
    current_seq=0,
    flush_seq=20,
    dbname,
    ddoc_id,
    def,
    def_lang,
    crs,
    name,
    type,
    dimension,
    limit=200,
    sig=nil
}).


-record(hq_args, {
    geom,
    nearest = false,
    filter,

    req_srid = 0,
    resp_srid = 0,
    
    start_time,
    end_time,

    limit = 200,
    skip = 0,
    stale = false,
    include_docs = false
}).


-record(docs, {
    update_seq,
    total_hits,
    hits
}).

