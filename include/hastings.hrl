-record(index, {
    current_seq=0,
    dbname,
    ddoc_id,
    def,
    def_lang,
    crs,
    name,
    idx,
    sig=nil
}).

-record(index_query_args, {
    q,
    limit=25,
    stale=false,
    include_docs=false,
    bookmark=nil,
    sort=relevance
}).

-record(top_docs, {
    update_seq,
    total_hits,
    hits
}).
-record(hit, {
    order,
    fields
}).
