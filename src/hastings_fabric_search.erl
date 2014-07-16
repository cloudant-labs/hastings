%% Copyright 2014 Cloudant

-module(hastings_fabric_search).


-include_lib("mem3/include/mem3.hrl").
-include_lib("couch/include/couch_db.hrl").
-include("hastings.hrl").


-export([
    go/4
]).


go(DbName, GroupId, IndexName, HQArgs) when is_binary(GroupId) ->
    {ok, DDoc} = fabric:open_doc(DbName, <<"_design/", GroupId/binary>>, []),
    go(DbName, DDoc, IndexName, HQArgs);

go(DbName, DDoc, IndexName, HQArgs) ->
    Shards = get_shards(DbName, HQArgs),
    Args = [fabric_util:doc_id_and_rev(DDoc), IndexName, HQArgs],
    Workers = fabric_util:submit_jobs(Shards, hastings_rpc, search, Args),
    Counters = fabric_dict:init(Workers, nil),
    RexiMon = fabric_util:create_monitors(Workers),
    St = #h_acc{
        counters = Counters,
        resps = []
    },
    CB = fun handle_message/3,
    try rexi_utils:recv(Workers, #shard.ref, CB, St, infinity, 3600000) of
        {ok, #h_acc{resps = Resps}} ->
            {ok, Resps};
        {error, Reason} ->
            {error, Reason}
    after
        rexi_monitor:stop(RexiMon),
        fabric_util:cleanup(Workers)
    end.


handle_message({ok, Resps}, Shard, St) ->
    case fabric_dict:lookup_element(Shard, St#h_acc.counters) of
        undefined ->
            {ok, St};
        nil ->
            C1 = fabric_dict:store(Shard, ok, St#h_acc.counters),
            C2 = fabric_view:remove_overlapping_shards(Shard, C1),
            NewSt = St#h_acc{
                counters = C2,
                resps = merge_resps(St#h_acc.resps, Resps)
            },
            case fabric_dict:any(nil, C2) of
                true -> {ok, NewSt};
                false -> {stop, NewSt}
            end
    end;

handle_message(Else, Worker, St) ->
    hastings_fabric:handle_error(Else, Worker, St).


get_shards(DbName, #h_args{stale=ok}) ->
    mem3:ushards(DbName);
get_shards(DbName, #h_args{stale=update_after}) ->
    mem3:ushards(DbName);
get_shards(DbName, _) ->
    mem3:shards(DbName).


merge_resps(RespsA, RespsB) ->
    RespsA ++ RespsB.
