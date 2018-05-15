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

-module(hastings_index_manager).
-behavior(gen_server).


-include_lib("couch/include/couch_db.hrl").
-include("hastings.hrl").


-define(BY_SIG, hastings_by_sig).
-define(BY_PID, hastings_by_pid).
-define(BY_DIR, hastings_by_dir).



-export([
    start_link/0,
    get_index/2,
    upgrade/0
]).

-export([
    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3
]).

-export([
    new_index/4
]).


-record(st, {
    generation = 0
}).


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


get_index(DbName, Index) ->
    gen_server:call(?MODULE, {get_index, DbName, Index}, infinity).


upgrade() ->
    gen_server:call(?MODULE, upgrade, infinity).


init([]) ->
    ets:new(?BY_SIG, [set, public, named_table]),
    ets:new(?BY_PID, [set, public, named_table]),
    ets:new(?BY_DIR, [set, public, named_table]),
    process_flag(trap_exit, true),
    {ok, #st{}}.


terminate(_Reason, _State) ->
    ok.


handle_call({get_index, DbName, #h_idx{sig=Sig}=Index}, From, State) ->
    Gen = State#st.generation,
    case ets:lookup(?BY_SIG, {DbName, Sig}) of
    [] ->
        spawn_link(?MODULE, new_index, [self(), DbName, Index, Gen]),
        ets:insert(?BY_SIG, {{DbName,Sig}, [From]}),
        {noreply, State};
    [{_, WaitList}] when is_list(WaitList) ->
        ets:insert(?BY_SIG, {{DbName, Sig}, [From | WaitList]}),
        {noreply, State};
    [{_, ExistingPid}] ->
        hastings_index:add_monitor(ExistingPid, From),
        {reply, {ok, ExistingPid}, State}
    end;

handle_call(upgrade, _From, State) ->
    Gen = State#st.generation,
    NewState = State#st{
        generation = Gen + 1
    },
    {reply, Gen + 1, NewState};

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


handle_cast({gen_check, FromPid, Gen}, State) ->
    case ets:lookup(?BY_PID, FromPid) of
        [] ->
            % An old index that we have since forgotten about.
            % Its probably just indexing so ignore it and it'll
            % go away on its own.
            ok;
        [{_, {DbName, Sig}}] ->
            CurrGen = Gen == State#st.generation,
            MaxOpen = ets:info(?BY_PID, size) > get_max_open(),
            case CurrGen andalso not MaxOpen of
                true ->
                    ok;
                false ->
                    case hastings_index:stop(FromPid) of
                        ok ->
                            delete_from_ets(FromPid, DbName, Sig);
                        false ->
                            ok
                    end
            end
    end,
    {noreply, State}.


handle_info({'EXIT', FromPid, Reason}, State) ->
    case ets:lookup(?BY_PID, FromPid) of
    [] ->
        if Reason =/= normal ->
            couch_log:error("Exit on non-updater process: ~p", [Reason]),
            exit(Reason);
        true -> ok
        end;
    [{_, {DbName, Sig}}] ->
        delete_from_ets(FromPid, DbName, Sig)
    end,
    {noreply, State}.


code_change(_OldVsn, Ref, _Extra) when is_reference(Ref) ->
    demonitor(Ref),
    {ok, nil};
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


new_index(Manager, DbName, #h_idx{sig=Sig}=Index, Generation) ->
    case hastings_index:start_link(Manager, DbName, Index, Generation) of
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


get_max_open() ->
    MO = config:get("hastings", "open_index_soft_limit", "100"),
    try
        list_to_integer(MO)
    catch _:_ ->
        100
    end.
