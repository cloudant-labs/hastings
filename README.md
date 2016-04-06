hastings
========
Geospatial Search

Hastings consists of the following files:

- **hastings.app.src** - application resource file. As can be seen from this file, a callback module for the application is hastings_app, and the two registered processes started in this application are: hastings_index_manager and hastings_sup.
- **hastings_app.erl** - a callback module for the application that starts the top supervisor by hastings_sup:start_link().
- **hastings_sup.erl** - the top supervisor that starts hastings_index_manager and hastings_vacuum as its child worker process.
- **hastings_index_manager.erl** - manages multiple processes of hastings_index, and provides new_index, get_index as well as upgrade capabilities.
- **hastings_index.erl** - contains main callback functions to operate on index. One process is created for every index (a distinct index function in a design document).
- **hastings_index_updater.erl** - contains callback functions for index update.
- **hastings_httpd.erl** - handles http requests.
- **hastings_fabric.erl**, hastings_fabric_info.erl, hastings_fabric_search.erl - collection of proxy functions for operations in a cluster with shards.
- **hastings_format.erl**, hastings_format_geojson.erl, hastings_format_legacy.erl,hastings_format_legacy_local.erl- collection of format conversion functions.
- **hastings_rpc.erl** - proxy functions executed for every shard.
- **hastings_bookmark.erl** - utility functions for managing bookmarks for retrieving the next set of results
- **hastings_util.erl** - various utility functions


Life of http request
------------
Http requests have the following life cycle：
1. A request from chttpd goes to hastings_httpd.

2. hastings_httpd:
passes and validates the request in functions: check_enabled & parse_search_req.
Depending on the type of the request, invokes one of the fabric_functions or hastings_vacuum: 
handle_search_req -> hastings_fabric_search
handle_info_req -> hastings_fabric_info
cleanup_req -> hastings_vacuum.

3. hastings_fabric:
Get shards to be executed on every shard:
Shards = hastings_fabric:get_shards(DbName, Primary0, Secondary0) 
spawns processes to execute jobs on every shard using a RPC server rexi: 
Counters = lists:foldl(fun(Shard, C) -> NewWorker = start_worker(St, Shard), fabric_dict:store(NewWorker, nil, C) end, fabric_dict:init([], nil), Shards),
rexi_utils:recv(Workers, #shard.ref, fun, State, infinity, 3600000)

4. hastings_rpc:
is executed on every shard of every node at the same time.
calls hastings_index_manager:get_index(DbName, Index) to get index. hastings_index_manager will spawn a process of creating an index if the index doesn't exist.
an index of every shard will be updated if necessary with an instruction hastings_index:await(Pid, AwaitSeq).
calls hastings_index:search(Pid, HQArgs) with a corresponding search request.

5. hastings_index:
synchronously calls easton_index:search.

6. easton_index:
calls cmd(Index, ?EASTON_COMMAND_SEARCH, Arg)  to run search in external application (easton_index, written in C++) using port. Errors thrown in the external application are passed back to the user and to log files.

7. results are returned from external application

8. results are passed to easton_index

9. results are passed to hastings_rpc

10. hastings_rpc processes pass their individual results as a reply rexi:reply(Result) to the initial hastings_fabric process that spawned them.

11. hastings_fabric merges results from all shards: Hits0 = merge_resps(Resps, HQArgs) and returns the results to hastings_httpd.

12. hastings_httpd returns the formatted results to chttpd through send_json(..)

Indexing
-------------

### Indexing triggered by a search request
During a search request, before hastings_rpc calls hastings_index:search, hastings_rpc first initiates the updating of Geospatial indexes. It does it in the following way:

1. The last sequence number (signifying the number of the last change in the database) is calculated: `AwaitSeq=get_await_seq(Shard#shard.name, HQArgs`. For the stale queries (queries that don't need to reflect recent changes in the database), AwaitSeq will be 0, meaning that they don't need to initiate update of the index, before returning query results. The meaning of 0 is 'wait until index is at least at update_seq 0' which is true even for empty indexes.

2. Function call  `hastings_index:design_doc_to_index(DDoc, IndexName)` returns a record representation of an index:
    ```
    #index{
        ddoc_id=Id,
        name=IndexName,
        def=Def,
        lang=Language,
        type=Type,
        dimensions=Dimensions,
        srid=Srid
        }
    ```
`Sig` here is a hashed version of an index function and an analyzer represented in a Javascript function in a design document. `Sig` is used to check if an index description is changed, and the index needs to be reconstructed.

3. Function call `hastings_index_manager:get_index(DbName, Index)` will return Pid of the corresponding to this index hastings_index process. hastings_index_manager stores all the hastings_index processes for all indexes in the storage: `ets:new(?BY_SIG, [set, public, named_table])`. If the hastings_index process of the given index exists in the ets ?BY_SIG, it will be returned. If it doesn't exist, a new hastings_index process will be spawned.  For this, hastings_index_manager in the `handle_call({get_index,..)` will return `{noreply, State};` to not block gen_server, and will transfer handling creation of a new index process to the spawned process - `spawn_link(fun() -> new_index(DbName, Index) end)`, remembering the Pid of the caller in the ets ?BY_SIG.  `new_index` will create a new index process, sending `open_ok` message to the hastings_index_manager gen_server. `handle_call({open_ok,..) ` will retrieve the Pid - `From` of the original caller, and send a reply to this caller, a message containing a Pid of the created index process - NewPid. Calling `add_to_ets(NewPid, DbName, Sig)` will update two ets ?BY_SIG and ?BY_Pid.

4. `hastings_index:await(Pid, AwaitSeq)` will initiate the update of the index, if the requested AwaitSeq is bigger than the current Seq stored in the index. It will do this by calling `hastings_index_updater:update(IndexPid, Index)`.  Hastings_index_updater will load all documents, modified since last seq stored in the hastings index, and for every document will call `hastings_index:remove` to delete documents in Geospatial Lucene Index, or `hastings_index:update` to update an index in Java GeoSpatial Index.

Metrics
--
Hastings metrics are described in the following brief descriptions

https://github.com/cloudant/hastings/blob/master/priv/stats_descriptions.cfg

And in the following FB case

https://cloudant.fogbugz.com/f/cases/55527/Add-histogram-of-time-to-index-document-to-geospatial-metrics#BugEvent.523768

Logs
--
Hastings log messages are generated and find their way to splunk, they can be searched for simply with a "hastings" search term, or narrowed by cluster.
