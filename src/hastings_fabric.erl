%% Copyright 2014 Cloudant

-module(hastings_fabric).


-include("hastings.hrl").


-export([
    handle_error/3
]).


handle_error({rexi_DOWN, _, {_, NodeRef}, _}, _, St) ->
    case fabric_util:remove_down_workers(St#h_acc.counters, NodeRef) of
        {ok, NewCounters} ->
            {ok, St#h_acc{counters = NewCounters}};
        error ->
            {error, {nodedown, <<"progress not possible">>}}
    end;
handle_error({rexi_EXIT, Reason}, Worker, State) ->
    handle_error_int(Reason, Worker, State);
handle_error({error, Reason}, Worker, State) ->
    handle_error_int(Reason, Worker, State);
handle_error({'EXIT', Reason}, Worker, State) ->
    handle_error_int({exit, Reason}, Worker, State);
handle_error(Else, Worker, State) ->
    handle_error_int(Else, Worker, State).


handle_error_int(Reason, Worker, St) ->
    Counters = fabric_dict:erase(Worker, St#h_acc.counters),
    case fabric_view:is_progress_possible(Counters) of
        true ->
            {ok, St#h_acc{counters = Counters}};
        false ->
            {error, Reason}
    end.
