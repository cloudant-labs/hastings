%% -*- erlang-indent-level: 4;indent-tabs-mode: nil -*-
%% Copyright 2012 Cloudant

-module(hastings_fabric_search).

-include("hastings.hrl").
-include_lib("mem3/include/mem3.hrl").
-include_lib("couch/include/couch_db.hrl").

-export([go/4, pack_bookmark/1]).

-record(state, {
    limit,
    sort,
    top_docs,
    counters
}).

go(DbName, GroupId, IndexName, QueryArgs) when is_binary(GroupId) ->
    {ok, DDoc} = fabric:open_doc(DbName, <<"_design/", GroupId/binary>>, []),
    go(DbName, DDoc, IndexName, QueryArgs);

go(DbName, DDoc, IndexName, #index_query_args{bookmark=nil}=QueryArgs) ->
    Shards = get_shards(DbName, QueryArgs),
    Workers = fabric_util:submit_jobs(Shards, hastings_rpc, search, [DDoc, IndexName, QueryArgs]),
    Counters = fabric_dict:init(Workers, nil),
    Bookmark = Counters,
    go(QueryArgs, Counters, Bookmark);

go(DbName, DDoc, IndexName, QueryArgs) ->
    Bookmark = unpack_bookmark(DbName, QueryArgs),
    Shards = get_shards(DbName, QueryArgs),
    LiveNodes = [node() | nodes()],
    LiveShards = [S || #shard{node=Node} = S <- Shards, lists:member(Node, LiveNodes)],
    Counters = lists:flatmap(fun({#shard{name=Name, node=N} = Shard, Bookmark1}) ->
        QueryArgs1 = QueryArgs#index_query_args{bookmark=Bookmark1},
        case lists:member(Shard, LiveShards) of
        true ->
            Ref = rexi:cast(N, {hastings_rpc, search, [Name, DDoc, IndexName, QueryArgs1]}),
            [{Shard#shard{ref = Ref}, nil}];
        false ->
            lists:map(fun(#shard{name=Name2, node=N2} = NewShard) ->
                Ref = rexi:cast(N2, {hastings_rpc, search, [Name2, DDoc, IndexName, QueryArgs1]}),
                {NewShard#shard{ref = Ref}, nil}
            end, find_replacement_shards(Shard, LiveShards))
        end
    end, Bookmark),
    go(QueryArgs, Counters, Bookmark).

go(QueryArgs, Counters, Bookmark) ->
    {Workers, _} = lists:unzip(Counters),
    #index_query_args{limit = Limit, sort = Sort} = QueryArgs,
    State = #state{
        limit = Limit,
        sort = Sort,
        top_docs = #top_docs{total_hits=0,hits=[]},
        counters = Counters
     },
    RexiMon = fabric_util:create_monitors(Workers),
    try rexi_utils:recv(Workers, #shard.ref, fun handle_message/3,
        State, infinity, 1000 * 60 * 60) of
    {ok, #state{top_docs=#top_docs{total_hits=TotalHits, hits=Hits}}} ->
        Bookmark1 = create_bookmark(Sort, Bookmark, Hits),
        {ok, Bookmark1, TotalHits, element(1, lists:unzip(Hits))};
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

handle_message({ok, #top_docs{hits=Hits}=NewTopDocs0}, Shard, #state{top_docs=TopDocs, limit=Limit, sort=Sort}=State) ->
    NewTopDocs = NewTopDocs0#top_docs{hits=[{Hit,Shard} || Hit <- Hits]},
    case fabric_dict:lookup_element(Shard, State#state.counters) of
    undefined ->
        %% already heard from someone else in this range
        {ok, State};
    nil ->
        C1 = fabric_dict:store(Shard, ok, State#state.counters),
        C2 = fabric_view:remove_overlapping_shards(Shard, C1),
        MergedTopDocs = merge_top_docs(TopDocs, NewTopDocs, Limit, Sort),
        State1 = State#state{
            counters=C2,
            top_docs=MergedTopDocs
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

find_replacement_shards(#shard{range=Range}, AllShards) ->
    [Shard || Shard <- AllShards, Shard#shard.range =:= Range].

merge_top_docs(#top_docs{total_hits=TotalA, hits=HitsA}, #top_docs{total_hits=TotalB, hits=HitsB}, Limit, Sort) ->
    MergedTotal = TotalA + TotalB,
    MergedHits = lists:sublist(lists:sort(fun(A, B) -> sort_top_docs(Sort, A, B) end, HitsA ++ HitsB), Limit),
    #top_docs{total_hits=MergedTotal, hits=MergedHits}.

sort_top_docs(relevance, A, B) ->
    sort_top_docs2([<<"-">>, <<"">>], A, B);
sort_top_docs(Sort, A, B) when is_binary(Sort) ->
    sort_top_docs2([Sort, <<"">>], A, B);
sort_top_docs(Sort, A, B) when is_list(Sort) ->
    sort_top_docs2(Sort ++ [<<"">>], A, B).

sort_top_docs2([<<"-",_/binary>>|_], {#hit{order=[A|_]},_}, {#hit{order=[B|_]},_}) when A =/= B ->
    A > B;
sort_top_docs2([_|_], {#hit{order=[A|_]},_}, {#hit{order=[B|_]},_}) when A =/= B ->
    A < B;
sort_top_docs2([], {_,#shard{name=A}}, {_,#shard{name=B}}) -> % arbitrary tie-break
    A =< B;
sort_top_docs2([_|Rest], {#hit{order=[_|RestA]}=HitA,ShardA}, {#hit{order=[_|RestB]}=HitB,ShardB}) ->
    sort_top_docs2(Rest, {HitA#hit{order=RestA}, ShardA}, {HitB#hit{order=RestB}, ShardB}).

pack_bookmark(nil) ->
    null;
pack_bookmark(Workers) ->
    Bin = term_to_binary(dedupe(Workers), [compressed, {minor_version,1}]),
    couch_util:encodeBase64Url(Bin).

dedupe(List) ->
    dedupe(List, []).

dedupe([], Acc) ->
    lists:reverse(Acc);
dedupe([{#shard{node=N, range=R},A}|Rest], Acc) ->
    case lists:keymember(R, 2, Acc) of
    true ->
        dedupe(Rest, Acc);
    false ->
        dedupe(Rest, [{N,R,A}|Acc])
    end.

unpack_bookmark(DbName, #index_query_args{bookmark=nil}=Args) ->
    fabric_dict:init(get_shards(DbName, Args), nil);
unpack_bookmark(DbName, #index_query_args{bookmark=Packed}) ->
    lists:map(fun({Node, Range, After}) ->
        case mem3:get_shard(DbName, Node, Range) of
        {ok, Shard} ->
            {Shard, After};
        {error, not_found} ->
            PlaceHolder = #shard{node=Node, range=Range, dbname=DbName, _='_'},
            {PlaceHolder, After}
        end
    end, binary_to_term(couch_util:decodeBase64Url(Packed))).

create_bookmark(_Sort, Bookmark, []) ->
    Bookmark;
create_bookmark(relevance, Bookmark, [{#hit{order=[Score, Doc]}, Shard}|Rest]) ->
    B1 = fabric_dict:store(Shard, {Score, Doc}, Bookmark),
    B2 = fabric_view:remove_overlapping_shards(Shard, B1),
    create_bookmark(relevance, B2, Rest);
create_bookmark(Sort, Bookmark, [{#hit{order=Order}, Shard}|Rest]) ->
    B1 = fabric_dict:store(Shard, Order, Bookmark),
    B2 = fabric_view:remove_overlapping_shards(Shard, B1),
    create_bookmark(Sort, B2, Rest).
