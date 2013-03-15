%% -*- erlang-indent-level: 4;indent-tabs-mode: nil -*-
%% Copyright 2012 Cloudant

-module(hastings_fabric_search).

-include("hastings.hrl").
-include_lib("mem3/include/mem3.hrl").
-include_lib("couch/include/couch_db.hrl").

-export([go/4]).

-record(state, {
    limit,
    docs,
    counters
}).

go(DbName, GroupId, IndexName, QueryArgs) when is_binary(GroupId) ->
    {ok, DDoc} = fabric:open_doc(DbName, <<"_design/", GroupId/binary>>, []),
    go(DbName, DDoc, IndexName, QueryArgs);

go(DbName, DDoc, IndexName, QueryArgs) ->
    Shards = get_shards(DbName, QueryArgs),
    Workers = fabric_util:submit_jobs(Shards, hastings_rpc, search, [DDoc, IndexName, QueryArgs]),
    Counters = fabric_dict:init(Workers, nil),
    go(QueryArgs, Counters).

go(QueryArgs, Counters) ->
    {Workers, _} = lists:unzip(Counters),
    #index_query_args{limit = Limit} = QueryArgs,
    State = #state{
        limit = Limit,
        docs = #docs{total_hits=0,hits=[]},
        counters = Counters
     },
    RexiMon = fabric_util:create_monitors(Workers),
    try rexi_utils:recv(Workers, #shard.ref, fun handle_message/3,
        State, infinity, 1000 * 60 * 60) of
    {ok, #state{docs=#docs{total_hits=TotalHits, hits=Hits}}} ->
        {ok, TotalHits, Hits};
    {ok, _} ->
        {ok, 0, []};
    {error, Reason} ->
        {error, Reason}
    after
        rexi_monitor:stop(RexiMon),
        fabric_util:cleanup(Workers)
    end.

handle_message({rexi_DOWN, _, {_, NodeRef}, _}, _, State) ->
    case fabric_util:remove_down_workers(State#state.counters, NodeRef) of
    {ok, NewCounters} ->
        {ok, State#state{counters=NewCounters}};
    error ->
        {error, {nodedown, <<"progress not possible">>}}
    end;

handle_message({ok, #docs{hits=Hits}=NewDocs0}, Shard, #state{docs=Docs, limit=Limit}=State) ->
    NewDocs = NewDocs0#docs{hits=[{Hit,Shard} || Hit <- Hits]},
    case fabric_dict:lookup_element(Shard, State#state.counters) of
    undefined ->
        %% already heard from someone else in this range
        {ok, State};
    nil ->
        C1 = fabric_dict:store(Shard, ok, State#state.counters),
        C2 = fabric_view:remove_overlapping_shards(Shard, C1),
        
        MergedDocs = merge_docs(Docs, NewDocs, Limit),
        State1 = State#state{
            counters=C2,
            docs=MergedDocs
        },

        case fabric_dict:any(nil, C2) of
        true ->
            {ok, State1};
        false ->
            {stop, State1}
        end
    end;

handle_message({rexi_EXIT, Reason}, Worker, State) ->
    handle_error(Reason, Worker, State);
handle_message({error, Reason}, Worker, State) ->
    handle_error(Reason, Worker, State);
handle_message({'EXIT', Reason}, Worker, State) ->
    handle_error({exit, Reason}, Worker, State).

handle_error(Reason, Worker, State) ->
    #state{
        counters=Counters0
    } = State,
    Counters = fabric_dict:erase(Worker, Counters0),
    case fabric_view:is_progress_possible(Counters) of
    true ->
        {ok, State#state{counters=Counters}};
    false ->
        {error, Reason}
    end.

get_shards(DbName, #index_query_args{stale=ok}) ->
    mem3:ushards(DbName);
get_shards(DbName, #index_query_args{stale=false}) ->
    mem3:shards(DbName).

merge_docs(#docs{total_hits=TotalA, hits=HitsA}, #docs{total_hits=TotalB, hits=HitsB}, Limit) ->
    MergedTotal = TotalA + TotalB,
    MergedHits = lists:sublist(HitsA ++ HitsB, Limit),
    #docs{total_hits=MergedTotal, hits=MergedHits}.