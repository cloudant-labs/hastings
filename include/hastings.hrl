-record(index, {
    current_seq=0,
    dbname,
    ddoc_id,
    def,
    def_lang,
    crs="urn:ogc:def:crs:EPSG::4326",
    name,
    sig=nil
}).

-record(index_query_args, {
    bbox=undefined,
    wkt=undefined,
    radius=undefined,
    lat=undefined,
    lon=undefined,
    limit=200,
    stale=false,
    include_docs=false,
    bookmark=nil
}).

-record(docs, {
    update_seq,
    total_hits,
    hits
}).

% CRS
-define(WGS84_LL, "urn:ogc:def:crs:EPSG::4326").

