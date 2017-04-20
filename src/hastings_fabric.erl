% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(hastings_fabric).


-include_lib("mem3/include/mem3.hrl").
-include("hastings.hrl").


-export([
    run/3,
    run/5
]).


-record(st, {
    nodes,
    counters,
    replacements,
    start_fun,
    start_args
}).


run(DbName, StartFun, StartArgs) ->
    run(DbName, StartFun, StartArgs, [], []).


run(DbName, StartFun, StartArgs, PrimaryShards, SecondaryShards) ->
    try
        St = init_state(DbName, StartFun, StartArgs,
                PrimaryShards, SecondaryShards),
        run(St)
    catch throw:Reason ->
        Reason
    end.


run(St) ->
    RexiMon = create_monitors(St),
    {Workers, _} = lists:unzip(St#st.counters),
    CB = fun handle_message/3,
    try rexi_utils:recv(Workers, #shard.ref, CB, St, infinity, 3600000) of
        {ok, NewSt} ->
            RespList = fabric_dict:to_list(NewSt#st.counters),
            {ok, [R || {_, R} <- RespList]};
        {error, Reason} ->
            {error, Reason}
    after
        rexi_monitor:stop(RexiMon),
        fabric_util:cleanup(Workers)
    end.


init_state(DbName, StartFun, StartArgs, PrimaryShards, SecondaryShards) ->
    {Nodes, Shards, Replacements} = get_shards(
            DbName, PrimaryShards, SecondaryShards),
    Counters0 = fabric_dict:init(Shards, nil),
    case fabric_view:is_progress_possible(Counters0) of
        true ->
            ok;
        false ->
            throw({error, {nodedown, <<"progress not possible">>}})
    end,
    St = #st{
        nodes = Nodes,
        replacements = Replacements,
        start_fun = StartFun,
        start_args = StartArgs
    },
    Counters = lists:foldl(fun(Shard, C) ->
        NewWorker = start_worker(St, Shard),
        fabric_dict:store(NewWorker, nil, C)
    end, fabric_dict:init([], nil), Shards),
    St#st{
        counters = Counters
    }.


handle_message({ok, _} = Resp, Shard, St) ->
    case fabric_dict:lookup_element(Shard, St#st.counters) of
        undefined ->
            % We already have a response for this range
            {ok, St};
        nil ->
            C1 = fabric_dict:store(Shard, Resp, St#st.counters),
            C2 = fabric_view:remove_overlapping_shards(Shard, C1),
            NewSt = St#st{counters = C2},
            case fabric_dict:any(nil, C2) of
                true -> {ok, NewSt};
                false -> {stop, NewSt}
            end
    end;

handle_message({rexi_DOWN, _, {_, NodeRef}, _}, _Worker, St) ->
    NewReplacements = filter_replacements(St#st.replacements, NodeRef),
    case fabric_util:remove_down_workers(St#st.counters, NodeRef) of
        {ok, NewCounters} ->
            {ok, St#st{
                counters = NewCounters,
                replacements = NewReplacements
            }};
        error ->
            {error, {nodedown, <<"progress not possible">>}}
    end;

handle_message({rexi_EXIT, {maintenance_mode, _}}, Worker, St) ->
    CurrCounters = lists:filter(fun({#shard{ref=Ref}, _}) ->
        Ref /= Worker#shard.ref
    end, St#st.counters),
    case lists:keytake(Worker#shard.range, 1, St#st.replacements) of
        {value, {_Range, Replacements}, NewReplacements} ->
            NewCounters = lists:foldl(fun(Repl, CounterAcc) ->
                NewWorker = start_worker(St, Repl),
                fabric_dict:store(NewWorker, nil, CounterAcc)
            end, CurrCounters, Replacements),
            true = fabric_view:is_progress_possible(NewCounters),
            NewRefs = fabric_dict:fetch_keys(NewCounters),
            NewSt = St#st{
                counters = NewCounters,
                replacements = NewReplacements
            },
            {new_refs, NewRefs, NewSt};
        false ->
            Error = {nodedown, <<"progress not possible">>},
            handle_error_int(Error, Worker, St)
    end;

handle_message({rexi_EXIT, Reason}, Worker, St) ->
    handle_error_int(Reason, Worker, St);

handle_message({'EXIT', Reason}, Worker, St) ->
    handle_error_int({exit, Reason}, Worker, St);

handle_message({error, Reason}, Worker, St) ->
    handle_error_int(Reason, Worker, St);

handle_message(Else, Worker, St) ->
    handle_error_int(Else, Worker, St).


handle_error_int(Reason, Worker, St) ->
    Counters = fabric_dict:erase(Worker, St#st.counters),
    case fabric_view:is_progress_possible(Counters) of
        true ->
            {ok, St#st{counters = Counters}};
        false ->
            {error, Reason}
    end.


start_worker(St, Shard) ->
    Msg = {hastings_rpc, St#st.start_fun, [Shard | St#st.start_args]},
    Ref = rexi:cast(Shard#shard.node, Msg),
    Shard#shard{ref = Ref}.


create_monitors(St) ->
    MonRefs = lists:usort([rexi_utils:server_pid(N) || N <- St#st.nodes]),
    rexi_monitor:start(MonRefs).


filter_replacements([], _Node) ->
    [];
filter_replacements([{Range, Shards} | Rest], Node) ->
    NewShards = [S || #shard{node=N} = S <- Shards, N /= Node],
    [{Range, NewShards} | filter_replacements(Rest, Node)].


get_shards(DbName, Primary0, Secondary0) ->
    LiveShards = mem3:live_shards(DbName, [node() | nodes()]),
    LiveNodes = [N || #shard{node=N} <- LiveShards],

    Primary = get_shards(Primary0, LiveShards),
    Secondary = get_shards(Secondary0, LiveShards),
    Tertiary = LiveShards -- (Primary ++ Secondary),

    Grouped = group_shards(Primary, Secondary, Tertiary),
    InitAndRepls = lists:map(fun({Range, [Init | Rest]}) ->
        {Init, {Range, lists:merge(Rest)}}
    end, Grouped),
    {Shards, Replacements} = lists:unzip(InitAndRepls),

    {LiveNodes, lists:merge(Shards), Replacements}.


get_shards(Shards, LiveShards) ->
    lists:flatmap(fun(Shard) ->
        get_shard(Shard, LiveShards)
    end, Shards).


get_shard(#shard{}=Shard, LiveShards) ->
    Filt = fun(#shard{}=S) -> S == Shard end,
    lists:filter(Filt, LiveShards);

get_shard({Node, Range}, LiveShards) ->
    Filt = fun(#shard{node=N, range=R}) -> N == Node andalso R == Range end,
    lists:filter(Filt, LiveShards).


group_shards(Primary, Secondary, Tertiary) ->
    PriR = [R || #shard{range=R} <- Primary],
    SecR = [R || #shard{range=R} <- Secondary],
    TerR = [R || #shard{range=R} <- Tertiary],
    Ranges = lists:usort(PriR ++ SecR ++ TerR),
    Filt = fun(Group) -> Group /= [] end,
    lists:map(fun(R) ->
        Ps = get_range_shards(Primary, R),
        Ss = get_range_shards(Secondary, R),
        Ts = get_range_shards(Tertiary, R),
        Groups = lists:filter(Filt, [Ps, Ss, Ts]),
        {R, Groups}
    end, Ranges).


get_range_shards([], _Range) ->
    [];
get_range_shards([#shard{range=Range} = Shard | Rest], Range) ->
    [Shard | get_range_shards(Rest, Range)];
get_range_shards([_ | Rest], Range) ->
    get_range_shards(Rest, Range).

