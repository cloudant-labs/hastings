%% Copyright 2014 Cloudant

-module(hastings_fabric_info).


-include("hastings.hrl").
-include_lib("mem3/include/mem3.hrl").
-include_lib("couch/include/couch_db.hrl").


-export([go/3]).


go(DbName, DDocId, IndexName) when is_binary(DDocId) ->
    {ok, DDoc} = fabric:open_doc(DbName, <<"_design/", DDocId/binary>>, []),
    go(DbName, DDoc, IndexName);


go(DbName, DDoc, IndexName) ->
    Shards = mem3:shards(DbName),
    Args = [fabric_util:doc_id_and_rev(DDoc), IndexName],
    Workers = fabric_util:submit_jobs(Shards, hastings_rpc, info, Args),
    RexiMon = fabric_util:create_monitors(Shards),
    Acc0 = {fabric_dict:init(Workers, nil), []},
    try
        fabric_util:recv(Workers, #shard.ref, fun handle_message/3, Acc0)
    after
        rexi_monitor:stop(RexiMon),
        fabric_util:cleanup(Workers)
    end.


handle_message({ok, Info}, Worker, {Counters, Acc}) ->
    case fabric_dict:lookup_element(Worker, Counters) of
        undefined ->
            {ok, {Counters, Acc}};
        nil ->
            C1 = fabric_dict:store(Worker, ok, Counters),
            C2 = fabric_view:remove_overlapping_shards(Worker, C1),
            case fabric_dict:any(nil, C2) of
                true ->
                    {ok, {C2, [Info|Acc]}};
                false ->
                    {stop, merge_results(lists:flatten([Info|Acc]))}
            end
    end;

handle_message(Else, Worker, St) ->
    hastings_fabric:handle_error(Else, Worker, St).


merge_results(Info) ->
    Dict = lists:foldl(fun({K, V}, D0) ->
        orddict:append(K, V, D0)
    end, orddict:new(), Info),
    orddict:fold(fun
        (disk_size, X, Acc) ->
            [{disk_size, lists:sum(X)} | Acc];
        (doc_count, X, Acc) ->
            [{doc_count, lists:sum(X)} | Acc];
        (_, _, Acc) ->
            Acc
    end, [], Dict).
