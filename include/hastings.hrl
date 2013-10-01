-record(index, {
    current_seq=0,
    flush_seq=20,
    dbname,
    ddoc_id,
    def,
    def_lang,
    crs=undefined,
    name,
    sig=nil
}).

-record(index_query_args, {
    bbox=undefined,
    relation=undefined,
    wkt=undefined,
    radius=undefined,
    lat=undefined,
    lon=undefined,
    limit=200,
    stale=false,
    include_docs=false,
    startIndex=0,
    currentPage=0
}).

-record(docs, {
    update_seq,
    total_hits,
    hits
}).

% CRS
-define(WGS84_LL, "urn:ogc:def:crs:EPSG::4326").

