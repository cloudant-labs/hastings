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

-record(index_query_args, {
    bbox,
    relation,
    wkt,
    radius,
    range_x,
    range_y,
    x,
    y,
    limit=200,
    stale=false,
    include_docs=false,
    startIndex=0,
    currentPage=0,
    srs=0,
    responseSrs=0,
    nearest=false,
    tStart,
    tEnd
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
-define(IDX_INDEXTYPE, 0).
-define(IDX_RTREE, 0).
-define(IDX_TPRTREE, 2).
-define(IDX_DIMENSION, 1).

-define(IDX_STORAGE, 3).
-define(IDX_DISK, 1).
