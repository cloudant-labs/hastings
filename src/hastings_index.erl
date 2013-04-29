%% -*- erlang-indent-level: 4;indent-tabs-mode: nil -*-
%% Copyright 2012 Cloudant

-module(hastings_index).
-behavior(gen_server).
-include_lib("couch/include/couch_db.hrl").
-include("hastings.hrl").


% public api.
-export([start_link/2, design_doc_to_index/2, 
        await/2, search/2, info/1, update_seq/2, delete/3, delete_index/1, update/3]).

% gen_server api.
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

% private definitions.
-record(state, {
    dbname,
    index,
    updater_pid=nil,
    index_pid=nil,
    index_h=nil,
    waiting_list=[]
}).

% public functions.
start_link(DbName, Index) ->
    proc_lib:start_link(?MODULE, init, [{DbName, Index}]).

await(Pid, MinSeq) ->
    gen_server:call(Pid, {await, MinSeq}, infinity).

search(Pid, QueryArgs) ->
    gen_server:call(Pid, {search, QueryArgs}, infinity).

info(Pid) ->
    gen_server:call(Pid, info, infinity).

update_seq(Pid, NewCurrSeq) ->
    gen_server:call(Pid, {new_seq, NewCurrSeq}, infinity).

delete(Pid, Id, Geom) ->
  gen_server:call(Pid, {delete, Id, Geom}, infinity).

delete_index(Pid) ->
  gen_server:call(Pid, delete_index, infinity).

update(Pid, Id, Geom) ->
  gen_server:call(Pid, {update, Id, Geom}, infinity).

% gen_server functions.

init({DbName, Index}) ->
    process_flag(trap_exit, true),
    case open_index(DbName, Index) of
        {ok, Pid, Idx, Seq} ->
            State=#state{
              dbname=DbName,
              index=Index#index{current_seq=Seq, dbname=DbName},
              index_pid=Pid,
              index_h = Idx
             },
            {ok, Db} = couch_db:open_int(DbName, []),
            try couch_db:monitor(Db) after couch_db:close(Db) end,
            proc_lib:init_ack({ok, self()}),
            gen_server:enter_loop(?MODULE, [], State);
        Error ->
            proc_lib:init_ack(Error)
    end.

handle_call({await, RequestSeq}, From,
            #state{
                index=#index{current_seq=Seq}=Index,
                index_pid=IndexPid,
                updater_pid=nil,
                waiting_list=WaitList
            }=State) when RequestSeq > Seq ->
    UpPid = spawn_link(fun() -> hastings_index_updater:update(IndexPid, Index) end),
    {noreply, State#state{
        updater_pid=UpPid,
        waiting_list=[{From,RequestSeq}|WaitList]
    }};
handle_call({await, RequestSeq}, _From,
            #state{index=#index{current_seq=Seq}}=State) when RequestSeq =< Seq ->
    {reply, ok, State};
handle_call({await, RequestSeq}, From, #state{waiting_list=WaitList}=State) ->
    {noreply, State#state{
        waiting_list=[{From,RequestSeq}|WaitList]
    }};

handle_call({search, #index_query_args{bbox=undefined, wkt=undefined}=QueryArgs},
      _From, State = #state{index_h=Idx, index=#index{crs=Crs}}) ->
    #index_query_args{
        radius = Radius,
        lat = Lat,
        lon = Lon,
        limit = _Limit,
        stale = _Stale
    } = QueryArgs,
    % TODO paging
    case erl_spatial:index_intersects(Idx, {Lon, Lat, Radius},
      ?WGS84_LL, Crs) of 
    {ok, Hits} ->
      {reply, {ok, #docs{total_hits=length(Hits), hits=Hits}}, State};
    Reply ->
      {reply, Reply, State}
    end;

handle_call({search, #index_query_args{bbox=undefined}=QueryArgs}, _From,
     State = #state{index_h=Idx, index=#index{crs=Crs}}) ->
    #index_query_args{
        wkt = Wkt,
        limit = _Limit,
        stale = _Stale
    } = QueryArgs,
    % TODO paging
    case erl_spatial:index_intersects(Idx, Wkt, ?WGS84_LL, Crs) of 
    {ok, Hits} ->
      {reply, {ok, #docs{total_hits=length(Hits), hits=Hits}}, State};
    Reply ->
      {reply, Reply, State}
    end;

handle_call({search, QueryArgs}, _From, 
      State = #state{index_h=Idx, index=#index{crs=Crs}}) ->
    #index_query_args{
        bbox = BBox,
        limit = _Limit,
        stale = _Stale
    } = QueryArgs,
    % TODO paging
    Reply = case BBox of 
      [MinX, MinY, MaxX, MaxY] ->
          case erl_spatial:index_intersects(Idx,
            {MinX, MinY},
            {MaxX, MaxY},
            ?WGS84_LL, Crs
          ) of
          {ok, Hits} ->
            {ok, #docs{total_hits=length(Hits), hits=Hits}};
          R ->
            R
          end;
      _ ->
        {ok, #docs{total_hits=0, hits=[]}}
    end,
    {reply, Reply, State};

handle_call({new_seq, Seq}, _From, 
        #state{index=#index{dbname=DbName, sig=Sig}}=State) ->
    FileName = get_priv_filename(DbName, Sig),
    put_seq(FileName, Seq),
    {reply, {ok, updated}, State};

handle_call(info, _From, State = #state{index_h=Idx}) ->
    % get bounds
    {ok, [Min, Max]} = erl_spatial:index_bounds(Idx),
    {ok, DocCount} = erl_spatial:index_intersects_count(Idx, 
                                              Min, Max),
    {reply, [{ok, [{doc_count, DocCount}]}], State};

handle_call({delete, Id, Geom}, _From, State = #state{index_h=Idx}) ->
    {reply, erl_spatial:index_delete(Idx, Id, Geom), State};

handle_call(delete_index, _From, #state{index_h={dbname=DbName,
                                                 sig=Sig} = Idx}) ->
    erl_spatial:index_destroy(Idx),
    % delete geo priv as well
    PrivFileName = get_priv_filename(DbName, Sig),
    file:delete(PrivFileName),
    {reply, ok};

handle_call({update, Id, Geom}, _From, State = #state{index_h=Idx}) ->
    {reply, erl_spatial:index_insert(Idx, Id, Geom), State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'EXIT', FromPid, {updated, NewSeq}},
            #state{
              index=Index0,
              index_pid=IndexPid,
              updater_pid=UpPid,
              waiting_list=WaitList
             }=State) when UpPid == FromPid ->
    Index = Index0#index{current_seq=NewSeq},
    case reply_with_index(Index, WaitList) of
    [] ->
        {noreply, State#state{index=Index,
                              updater_pid=nil,
                              waiting_list=[]
                             }};
    StillWaiting ->
        Pid = spawn_link(fun() -> hastings_index_updater:update(IndexPid, Index) end),
        {noreply, State#state{index=Index,
                              updater_pid=Pid,
                              waiting_list=StillWaiting
                             }}
    end;
handle_info({'EXIT', _, {updated, _}}, State) ->
    {noreply, State};
handle_info({'EXIT', FromPid, Reason}, #state{
              index=Index,
              index_pid=IndexPid,
              waiting_list=WaitList
             }=State) when FromPid == IndexPid ->
    twig:log(notice, "index for ~p closed with reason ~p", [index_name(Index), Reason]),
    [gen_server:reply(Pid, {error, Reason}) || {Pid, _} <- WaitList],
    {stop, normal, State};
handle_info({'EXIT', FromPid, Reason}, #state{
              index=Index,
              updater_pid=UpPid,
              waiting_list=WaitList
             }=State) when FromPid == UpPid ->
    ?LOG_INFO("Shutting down index server ~p, updater ~p closing w/ reason~n~p",
        [index_name(Index), UpPid, Reason]),
    [gen_server:reply(Pid, {error, Reason}) || {Pid, _} <- WaitList],
    {stop, normal, State};
handle_info({'DOWN',_,_,Pid,Reason}, #state{
              index=Index,
              waiting_list=WaitList
             }=State) ->
    ?LOG_INFO("Shutting down index server ~p, db ~p closing w/ reason~n~p",
        [index_name(Index), Pid, Reason]),
    [gen_server:reply(P, {error, Reason}) || {P, _} <- WaitList],
    {stop, normal, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

% private functions.
open_index(DbName, #index{sig=Sig, crs=CRS}) ->
    FileName = get_filename(DbName, Sig), 
    case filelib:ensure_dir(FileName) of 
    ok ->
        case erl_spatial:index_create(FileName, CRS) of 
          {ok, Idx} ->
            case get_seq(get_priv_filename(DbName, Sig)) of
              {ok, Seq} ->
                {ok, self(), Idx, Seq};
              Error ->
                Error
            end; 
          Error ->
            Error
        end;
    Error ->
        Error
    end.


get_path(DbName) ->
    filename:join([couch_config:get("couchdb", "view_index_dir"), DbName]).

get_filename(DbName, Sig) ->
    filename:join([get_path(DbName), <<Sig/binary, ".geo">>]). 

get_priv_filename(DbName, Sig) ->
  FileName = get_filename(DbName, Sig),
  <<FileName/binary, ".geopriv">>.

get_seq(FileName) ->
    case file:consult(FileName) of 
        {ok, Terms} ->
            {ok, proplists:get_value(seq, Terms, 0)};
        _ ->
            put_seq(FileName, 0),
            {ok, 0}
    end.

put_seq(FileName, Seq) ->
    file:write_file(FileName, io_lib:fwrite("~p.\n",[[{seq, Seq}]])).

design_doc_to_index(#doc{id=Id,body={Fields}}, IndexName) ->
    Language = couch_util:get_value(<<"language">>, Fields, <<"javascript">>),
    {RawIndexes} = couch_util:get_value(<<"indexes">>, Fields, {[]}),
    case lists:keyfind(IndexName, 1, RawIndexes) of
        false ->
            {error, {not_found, <<IndexName/binary, " not found.">>}};
        {IndexName, {Index}} ->
            % if the Crs or design doc changes then it is a different index
            Crs = couch_util:get_value(<<"crs">>, Index, undefined),
            Def = couch_util:get_value(<<"index">>, Index),
            Sig = ?l2b(couch_util:to_hex(couch_util:md5(term_to_binary({Crs, Def})))),
            {ok, #index{
               ddoc_id=Id,
               def=Def,
               def_lang=Language,
               name=IndexName,
               crs=Crs,
               sig=Sig}}
    end.

reply_with_index(Index, WaitList) ->
    reply_with_index(Index, WaitList, []).

reply_with_index(_Index, [], Acc) ->
    Acc;
reply_with_index(#index{current_seq=IndexSeq}=Index, [{Pid, Seq}|Rest], Acc) when Seq =< IndexSeq ->
    gen_server:reply(Pid, ok),
    reply_with_index(Index, Rest, Acc);
reply_with_index(Index, [{Pid, Seq}|Rest], Acc) ->
    reply_with_index(Index, Rest, [{Pid, Seq}|Acc]).

index_name(#index{dbname=DbName,ddoc_id=DDocId,name=IndexName}) ->
    <<DbName/binary, " ", DDocId/binary, " ", IndexName/binary>>.
