-record(index, {
    current_seq=0,
    dbname,
    ddoc_id,
    def,
    def_lang,
    crs,
    name,
    sig=nil
}).

-record(index_query_args, {
    q,
    limit=25,
    stale=false,
    include_docs=false,
    bookmark=nil
}).

-record(docs, {
    update_seq,
    total_hits,
    hits
}).
