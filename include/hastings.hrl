-record(index, {
    current_seq=0,
    flush_seq=20,
    dbname,
    ddoc_id,
    def,
    def_lang,
    crs=undefined,
    name,
    limit=200,
    sig=nil
}).

-record(index_query_args, {
    bbox=undefined,
    relation=undefined,
    wkt=undefined,
    radius=undefined,
    range_x=undefined,
    range_y=undefined,
    x=undefined,
    y=undefined,
    limit=200,
    stale=false,
    include_docs=false,
    startIndex=0,
    currentPage=0,
    srs=0,
    responseSrs=0
}).

-record(docs, {
    update_seq,
    total_hits,
    hits
}).

% CRS
-define(WGS84_LL, "urn:ogc:def:crs:EPSG::4326").

-define(IDX_OVERWRITE, 13).
-define(IDX_FILENAME, 20).
-define(IDX_RESULTLIMIT, 24).

-define(IDX_STORAGE, 3).
-define(IDX_DISK, 1).
