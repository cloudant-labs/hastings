%% Copyright 2014 Cloudant

-module(hastings_index).
-behavior(gen_server).


-include_lib("couch/include/couch_db.hrl").
-include("hastings.hrl").


-export([
    start_link/2,
    design_doc_to_index/2,
    await/2,
    search/2,
    info/1,
    update_seq/2,
    delete/3,
    delete_index/1,
    update/3
]).

-export([
    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3
]).


-record(state, {
    dbname,
    index,
    updater_pid=nil,
    index_pid=nil,
    index_h=nil,
    waiting_list=[]
}).


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


init({DbName, Index}) ->
    process_flag(trap_exit, true),
    % Crs is defined at the database level
    Crs = try
        case mem3_shards:config_for_db(DbName) of
            {ok, not_found} ->
                "urn:ogc:def:crs:EPSG::4326";
            {ok, Config} ->
                case lists:keyfind(srs, 1, Config) of
                    {srs, Srs} ->
                        Srs;
                    false ->
                        "urn:ogc:def:crs:EPSG::4326"
                end
            end
    catch _:_ ->
        "urn:ogc:def:crs:EPSG::4326"
    end,

    case open_index(DbName, Index#index{crs=Crs}) of
        {ok, Pid, Idx, Seq, FlushSeq} ->
            State=#state{
                dbname=DbName,
                index=Index#index{
                    crs=Crs,
                    current_seq=Seq,
                    dbname=DbName,
                    flush_seq=FlushSeq
                },
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

handle_call({await,RequestSeq}, From, St) ->
    #state{
        index = #index{current_seq = Seq} = Index,
        index_pid = IndexPid,
        updater_pid = UpPid,
        waiting_list = WaitList
    } = St,
    case {RequestSeq, Seq, UpPid} of
        _ when RequestSeq =< Seq ->
            {reply, ok, St};
        _ when RequestSeq > Seq andalso UpPid == undefined ->
            UpPid = spawn_link(fun() ->
                hastings_index_updater:update(IndexPid, Index)
            end),
            {noreply, St#state{
                updater_pid = UpPid,
                waiting_list = [{From, RequestSeq} | WaitList]
            }};
        _ when RequestSeq > Seq andalso UpPid /= undefined ->
            {noreply, St#state{
                waiting_list = [{From, RequestSeq} | WaitList]
            }}
    end;

handle_call({search, #index_query_args{bbox=undefined, nearest=false, wkt=undefined,
      range_x=undefined, range_y=undefined, limit=ResultLimit}=QueryArgs},
      _From, State = #state{index_h=Idx, index=#index{crs=Crs}}) ->
    #index_query_args{
        radius = Radius,
        relation = Relation,
        x = X,
        y = Y,
        srs=ReqSrs,
        currentPage = CurrentPage
    } = QueryArgs,
    erl_spatial:index_set_resultset_offset(Idx, CurrentPage * ResultLimit),

    ResultSet = case Relation of
      "contains" ->
        erl_spatial:index_contains(Idx, {X, Y, Radius}, ReqSrs, Crs);
      "disjoint" ->
        erl_spatial:index_disjoint(Idx, {X, Y, Radius}, ReqSrs, Crs);
      "contains_properly" ->
        erl_spatial:index_contains_properly(Idx, {X, Y, Radius}, ReqSrs, Crs);
      "covered_by" ->
        erl_spatial:index_covered_by(Idx, {X, Y, Radius}, ReqSrs, Crs);
      "covers" ->
        erl_spatial:index_covers(Idx, {X, Y, Radius}, ReqSrs, Crs);
      "crosses" ->
        erl_spatial:index_crosses(Idx, {X, Y, Radius}, ReqSrs, Crs);
      "overlaps" ->
        erl_spatial:index_overlaps(Idx, {X, Y, Radius}, ReqSrs, Crs);
      "touches" ->
        erl_spatial:index_touches(Idx, {X, Y, Radius}, ReqSrs, Crs);
      "within" ->
        erl_spatial:index_within(Idx, {X, Y, Radius}, ReqSrs, Crs);
      _ ->
        erl_spatial:index_intersects(Idx, {X, Y, Radius}, ReqSrs, Crs)
    end,

    case ResultSet of
    {ok, Hits} ->
      {reply, {ok, #docs{total_hits=length(Hits), hits=Hits}}, State};
    Reply ->
      {reply, Reply, State}
    end;

handle_call({search, #index_query_args{bbox=undefined, nearest=false, range_x=undefined, range_y=undefined}=QueryArgs},
     _From, State = #state{index_h=Idx, index=#index{crs=Crs}}) ->
    #index_query_args{
        wkt = Wkt,
        relation = Relation,
        currentPage = CurrentPage,
        srs=ReqSrs,
        limit=ResultLimit
    } = QueryArgs,
    erl_spatial:index_set_resultset_offset(Idx, CurrentPage * ResultLimit),
    ResultSet = case Relation of
      "contains" ->
        erl_spatial:index_contains(Idx, Wkt, ReqSrs, Crs);
      "disjoint" ->
        erl_spatial:index_disjoint(Idx, Wkt, ReqSrs, Crs);
      "contains_properly" ->
        erl_spatial:index_contains_properly(Idx, Wkt, ReqSrs, Crs);
      "covered_by" ->
        erl_spatial:index_covered_by(Idx, Wkt, ReqSrs, Crs);
      "covers" ->
        erl_spatial:index_covers(Idx, Wkt, ReqSrs, Crs);
      "crosses" ->
        erl_spatial:index_crosses(Idx, Wkt, ReqSrs, Crs);
      "overlaps" ->
        erl_spatial:index_overlaps(Idx, Wkt, ReqSrs, Crs);
      "touches" ->
        erl_spatial:index_touches(Idx, Wkt, ReqSrs, Crs);
      "within" ->
        erl_spatial:index_within(Idx, Wkt, ReqSrs, Crs);
      _ ->
        erl_spatial:index_intersects(Idx, Wkt, ReqSrs, Crs)
    end,

    case ResultSet of
    {ok, Hits} ->
      {reply, {ok, #docs{total_hits=length(Hits), hits=Hits}}, State};
    Reply ->
      {reply, Reply, State}
    end;

handle_call({search, #index_query_args{bbox=undefined, nearest=false}=QueryArgs}, _From,
     State = #state{index_h=Idx, index=#index{crs=Crs}}) ->
    #index_query_args{
        range_x=RangeX,
        range_y=RangeY,
        x=X,
        y=Y,
        relation = Relation,
        currentPage = CurrentPage,
        srs=ReqSrs,
        limit=ResultLimit
    } = QueryArgs,
    erl_spatial:index_set_resultset_offset(Idx, CurrentPage * ResultLimit),

    ResultSet = case Relation of
      "contains" ->
        erl_spatial:index_contains(Idx, {X, Y, RangeX, RangeY}, ReqSrs, Crs);
      "disjoint" ->
        erl_spatial:index_disjoint(Idx, {X, Y, RangeX, RangeY}, ReqSrs, Crs);
      "contains_properly" ->
        erl_spatial:index_contains_properly(Idx, {X, Y, RangeX, RangeY}, ReqSrs, Crs);
      "covered_by" ->
        erl_spatial:index_covered_by(Idx, {X, Y, RangeX, RangeY}, ReqSrs, Crs);
      "covers" ->
        erl_spatial:index_covers(Idx, {X, Y, RangeX, RangeY}, ReqSrs, Crs);
      "crosses" ->
        erl_spatial:index_crosses(Idx, {X, Y, RangeX, RangeY}, ReqSrs, Crs);
      "overlaps" ->
        erl_spatial:index_overlaps(Idx, {X, Y, RangeX, RangeY}, ReqSrs, Crs);
      "touches" ->
        erl_spatial:index_touches(Idx, {X, Y, RangeX, RangeY}, ReqSrs, Crs);
      "within" ->
        erl_spatial:index_within(Idx, {X, Y, RangeX, RangeY}, ReqSrs, Crs);
      _ ->
        erl_spatial:index_intersects(Idx, {X, Y, RangeX, RangeY}, ReqSrs, Crs)
    end,

    case ResultSet of
    {ok, Hits} ->
      {reply, {ok, #docs{total_hits=length(Hits), hits=Hits}}, State};
    Reply ->
      {reply, Reply, State}
    end;

handle_call({search, #index_query_args{nearest=true}=QueryArgs}, _From,
      State = #state{index_h=Idx, index=#index{crs=Crs}}) ->
    #index_query_args{
        bbox = BBox,
        currentPage = CurrentPage,
        limit=ResultLimit,
        srs=ReqSrs
    } = QueryArgs,
    erl_spatial:index_set_resultset_offset(Idx, CurrentPage * ResultLimit),
    % Z, M support
    {Min, Max} = lists:split(length(BBox) div 2, BBox),
    Reply = case erl_spatial:index_nearest(Idx,
        list_to_tuple(Min),
        list_to_tuple(Max),
        ReqSrs, Crs) of
      {ok, Hits} ->
        {ok, #docs{total_hits=length(Hits), hits=Hits}};
      R ->
        R
    end,
    {reply, Reply, State};

handle_call({search, QueryArgs=#index_query_args{tStart=undefined, tEnd=undefined}}, _From,
      State = #state{index_h=Idx, index=#index{crs=Crs}}) ->
    #index_query_args{
        bbox = BBox,
        currentPage = CurrentPage,
        limit=ResultLimit,
        srs=ReqSrs
    } = QueryArgs,
    erl_spatial:index_set_resultset_offset(Idx, CurrentPage * ResultLimit),
    % Z, M support
    {Min, Max} = lists:split(length(BBox) div 2, BBox),
    Reply = case erl_spatial:index_intersects_mbr(Idx,
        list_to_tuple(Min),
        list_to_tuple(Max),
        ReqSrs, Crs) of
      {ok, Hits} ->
        {ok, #docs{total_hits=length(Hits), hits=Hits}};
      R ->
        R
    end,
    {reply, Reply, State};

handle_call({search, QueryArgs}, _From,
      State = #state{index_h=Idx, index=#index{crs=Crs}}) ->
    #index_query_args{
        bbox = BBox,
        currentPage = CurrentPage,
        limit=ResultLimit,
        srs=ReqSrs,
        tStart=Start,
        tEnd=End
    } = QueryArgs,
    erl_spatial:index_set_resultset_offset(Idx, CurrentPage * ResultLimit),
    {Min, Max} = lists:split(length(BBox) div 2, BBox),
    Reply = case erl_spatial:index_intersects_mbr(Idx,
        list_to_tuple(Min),
        list_to_tuple(Max),
        Start, End,
        ReqSrs, Crs) of
      {ok, Hits} ->
        {ok, #docs{total_hits=length(Hits), hits=Hits}};
      R ->
        R
    end,
    {reply, Reply, State};


handle_call({new_seq, Seq}, _From,
    #state{index=#index{dbname=DbName, sig=Sig,
        flush_seq=_FlushSeq} = _Idx}=State) ->
    % when to flush index
    put_seq(get_priv_filename(DbName, Sig), Seq),
    {reply, {ok, updated}, State};

handle_call(info, _From, State = #state{index_h=Idx}) ->
    % get bounds
    {ok, [Min, Max]} = erl_spatial:index_bounds(Idx),
    {ok, DocCount} = erl_spatial:index_intersects_count(Idx,
                                              Min, Max),
    {reply, [{ok, [{doc_count, DocCount}]}], State};

handle_call({delete, Id, {Val}}, _From, State = #state{index_h=Idx, index=#index{type=tprtree}}) ->
    MinV = couch_util:get_value(<<"lowV">>, Val, []),
    MaxV = couch_util:get_value(<<"maxV">>, Val, []),
    Start = couch_util:get_value(<<"start">>, Val, 0),
    End = couch_util:get_value(<<"end">>, Val, 0),
    Type = couch_util:get_value(<<"type">>, Val, []),
    Coords = couch_util:get_value(<<"coordinates">>, Val, []),
    Geom = {[{<<"type">>, Type},
             {<<"coordinates">>, Coords}
            ]},
    {reply, erl_spatial:index_delete(Idx, Id, Geom,
      list_to_tuple(MinV), list_to_tuple(MaxV), Start, End), State};

handle_call({delete, Id, Geom}, _From, State = #state{index_h=Idx}) ->
    {reply, erl_spatial:index_delete(Idx, Id, Geom), State};

handle_call(delete_index, _From, #state{index_h={dbname=DbName,
                                                 sig=Sig} = Idx}) ->
    erl_spatial:index_destroy(Idx),
    % delete geo priv as well
    PrivFileName = get_priv_filename(DbName, Sig),
    file:delete(PrivFileName),
    {reply, ok};

handle_call({update, Id, {Val}}, _From, State = #state{index_h=Idx, index=#index{type=tprtree}}) ->
    MinV = couch_util:get_value(<<"lowV">>, Val, []),
    MaxV = couch_util:get_value(<<"highV">>, Val, []),
    Start = couch_util:get_value(<<"start">>, Val, 0),
    End = couch_util:get_value(<<"end">>, Val, 0),
    Type = couch_util:get_value(<<"type">>, Val, []),
    Coords = couch_util:get_value(<<"coordinates">>, Val, []),
    Geom = {[{<<"type">>, Type},
             {<<"coordinates">>, Coords}
            ]},
    case Reply = erl_spatial:index_insert(Idx, Id, Geom, list_to_tuple(MinV), list_to_tuple(MaxV), Start, End) of
      ok ->
        twig:log(error, "ok~n", []),
        erl_spatial:index_flush(Idx),
        {reply, Reply, State};
      _ ->
        {reply, Reply, State}
    end;

handle_call({update, Id, Geom}, _From, State = #state{index_h=Idx}) ->
    case Reply = erl_spatial:index_insert(Idx, Id, Geom) of
    ok ->
      erl_spatial:index_flush(Idx),
      {reply, Reply, State};
    _ ->
       {reply, Reply, State}
    end.

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
open_index(DbName, #index{sig=Sig, limit=Limit, type=Type, dimension=Dims}) ->
    FileName = get_filename(DbName, Sig),
    IndexType = case Type of
      tprtree ->
        ?IDX_TPRTREE;
      _ ->
        ?IDX_RTREE
    end,
    case filelib:ensure_dir(FileName) of
    ok ->
        case erl_spatial:index_create([{?IDX_STORAGE, ?IDX_DISK},
            {?IDX_FILENAME, binary_to_list(FileName)},
            {?IDX_RESULTLIMIT, Limit},
            {?IDX_INDEXTYPE, IndexType},
            {?IDX_DIMENSION, Dims},
            {?IDX_OVERWRITE, 0}]) of
          {ok, Idx} ->
            case get_seq(get_priv_filename(DbName, Sig)) of
              {ok, Seq} ->
                FlushSeq = list_to_integer(config:get("hastings", "flush_seq", "20")),
                {ok, self(), Idx, Seq, FlushSeq};
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
    filename:join([config:get("couchdb", "view_index_dir"), DbName]).

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
    file:write_file(FileName, io_lib:fwrite("~pq.\n",[[{seq, Seq}]])).

design_doc_to_index(#doc{id=Id,body={Fields}}, IndexName) ->
    % crs is defined at the database level
    % type and dimension part of the design doc.
    Language = couch_util:get_value(<<"language">>, Fields, <<"javascript">>),
    {RawIndexes} = couch_util:get_value(<<"indexes">>, Fields, {[]}),
    case lists:keyfind(IndexName, 1, RawIndexes) of
        false ->
            {error, {not_found, <<IndexName/binary, " not found.">>}};
        {IndexName, {Index}} ->
            % if the design doc, type and dimension changes then it is a different index
            Def = couch_util:get_value(<<"index">>, Index),
            Type = list_to_atom(?b2l(couch_util:get_value(<<"type">>, Index, <<"rtree">>))),
            Dims = couch_util:get_value(<<"dimension">>, Index, 2),
            Limit = couch_util:get_value(<<"limit">>, Index, 200),
            Sig = ?l2b(couch_util:to_hex(couch_util:md5(term_to_binary({Id, Def, Type, Dims})))),
            {ok, #index{
               ddoc_id=Id,
               def=Def,
               def_lang=Language,
               name=IndexName,
               type=Type,
               dimension=Dims,
               limit=Limit,
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
