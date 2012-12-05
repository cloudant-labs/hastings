%% -*- erlang-indent-level: 4;indent-tabs-mode: nil -*-
%% Copyright 2012 Cloudant

-module(hastings_index_manager).
-behavior(gen_server).
-include_lib("couch/include/couch_db.hrl").
-include("hastings.hrl").

-define(BY_SIG, hastings_by_sig).
-define(BY_PID, hastings_by_pid).

% public api.
-export([start_link/0, get_index/2]).

% gen_server api.
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

% public functions.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get_index(DbName, Index) ->
    gen_server:call(?MODULE, {get_index, DbName, Index}, infinity).

% gen_server functions.

init([]) ->
    ets:new(?BY_SIG, [set, private, named_table]),
    ets:new(?BY_PID, [set, private, named_table]),
    couch_db_update_notifier:start_link(
        fun({deleted, DbName}) ->
            gen_server:cast(?MODULE, {cleanup, DbName});
        ({created, DbName}) ->
            gen_server:cast(?MODULE, {cleanup, DbName});
        (_Else) ->
            ok
        end),
    process_flag(trap_exit, true),
    {ok, nil}.

handle_call({get_index, DbName, #index{sig=Sig}=Index}, From, State) ->
    case ets:lookup(?BY_SIG, {DbName, Sig}) of
    [] ->
        spawn_link(fun() -> new_index(DbName, Index) end),
        ets:insert(?BY_SIG, {{DbName,Sig}, [From]}),
        {noreply, State};
    [{_, WaitList}] when is_list(WaitList) ->
        ets:insert(?BY_SIG, {{DbName, Sig}, [From | WaitList]}),
        {noreply, State};
    [{_, ExistingPid}] ->
        {reply, {ok, ExistingPid}, State}
    end;

handle_call({open_ok, DbName, Sig, NewPid}, _From, State) ->
    link(NewPid),
    [{_, WaitList}] = ets:lookup(?BY_SIG, {DbName, Sig}),
    [gen_server:reply(From, {ok, NewPid}) || From <- WaitList],
    add_to_ets(NewPid, DbName, Sig),
    {reply, ok, State};

handle_call({open_error, DbName, Sig, Error}, _From, State) ->
    [{_, WaitList}] = ets:lookup(?BY_SIG, {DbName, Sig}),
    [gen_server:reply(From, Error) || From <- WaitList],
    ets:delete(?BY_SIG, {DbName, Sig}),
    {reply, ok, State}.

handle_cast({cleanup, DbName}, State) ->
    hastings_rpc:cleanup(DbName),
    {noreply, State}.

handle_info({'EXIT', FromPid, Reason}, State) ->
    case ets:lookup(?BY_PID, FromPid) of
    [] ->
        if Reason =/= normal ->
            ?LOG_ERROR("Exit on non-updater process: ~p", [Reason]),
            exit(reason);
        true -> ok
        end;
    [{_, {DbName, Sig}}] ->
        delete_from_ets(FromPid, DbName, Sig)
    end,
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, Ref, _Extra) when is_reference(Ref) ->
    demonitor(Ref),
    {ok, nil};
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

% private functions

new_index(DbName, #index{sig=Sig}=Index) ->
    case hastings_index:start_link(DbName, Index) of
    {ok, NewPid} ->
        Msg = {open_ok, DbName, Sig, NewPid},
        ok = gen_server:call(?MODULE, Msg, infinity),
        unlink(NewPid);
    Error ->
        Msg = {open_error, DbName, Sig, Error},
        ok = gen_server:call(?MODULE, Msg, infinity)
    end.

add_to_ets(Pid, DbName, Sig) ->
    true = ets:insert(?BY_PID, {Pid, {DbName, Sig}}),
    true = ets:insert(?BY_SIG, {{DbName, Sig}, Pid}).

delete_from_ets(Pid, DbName, Sig) ->
    true = ets:delete(?BY_PID, Pid),
    true = ets:delete(?BY_SIG, {DbName, Sig}).

