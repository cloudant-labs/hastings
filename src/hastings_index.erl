%% Copyright 2014 Cloudant

-module(hastings_index).
-behavior(gen_server).


-include_lib("couch/include/couch_db.hrl").
-include("hastings.hrl").


-export([
    start_link/2,
    destroy/1,
    design_doc_to_index/2,

    await/2,
    search/2,
    info/1,

    set_update_seq/2,
    update/3,
    remove/2
]).

-export([
    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3
]).


-record(st, {
    index,
    updater_pid,
    waiting_list = []
}).


start_link(DbName, Index) ->
    proc_lib:start_link(?MODULE, init, [{DbName, Index}]).


destroy(IndexDir) ->
    easton_index:destroy(IndexDir).


design_doc_to_index(#doc{id=Id, body={Fields}}, IndexName) ->
    try
        design_doc_to_index_int(Id, Fields, IndexName)
    catch throw:Error ->
        Error
    end.


await(Pid, MinSeq) ->
    gen_server:call(Pid, {await, MinSeq}, infinity).


search(Pid, QueryArgs) ->
    gen_server:call(Pid, {search, QueryArgs}, infinity).


info(Pid) ->
    gen_server:call(Pid, info, infinity).


set_update_seq(Pid, NewCurrSeq) ->
    gen_server:call(Pid, {new_seq, NewCurrSeq}, infinity).


update(Pid, Id, Geoms) ->
    gen_server:call(Pid, {update, Id, Geoms}, infinity).


remove(Pid, Id) ->
    gen_server:call(Pid, {remove, Id}, infinity).


init({DbName, Index}) ->
    process_flag(trap_exit, true),
    case open_index(DbName, Index) of
        {ok, NewIndex} ->
            {ok, Db} = couch_db:open_int(DbName, []),
            try
                couch_db:monitor(Db)
            after
                couch_db:close(Db)
            end,
            proc_lib:init_ack({ok, self()}),
            St = #st{index = NewIndex},
            gen_server:enter_loop(?MODULE, [], St);
        Error ->
            proc_lib:init_ack(Error)
    end.


terminate(_Reason, St) ->
    Index = St#st.index,
    catch exit(St#st.updater_pid, kill),
    ok = easton_index:close(Index#h_idx.pid).


handle_call({await, RequestSeq}, From, St) ->
    #st{
        index = #h_idx{update_seq = Seq} = Index,
        updater_pid = UpPid,
        waiting_list = WaitList
    } = St,
    case {RequestSeq, Seq, UpPid} of
        _ when RequestSeq =< Seq ->
            {reply, ok, St};
        _ when RequestSeq > Seq andalso UpPid == undefined ->
            Pid = spawn_link(hastings_index_updater, update, [self(), Index]),
            {noreply, St#st{
                updater_pid = Pid,
                waiting_list = [{From, RequestSeq} | WaitList]
            }};
        _ when RequestSeq > Seq andalso UpPid /= undefined ->
            {noreply, St#st{
                waiting_list = [{From, RequestSeq} | WaitList]
            }}
    end;

handle_call({search, HQArgs}, _From, St) ->
    Idx = St#st.index,
    Shape = HQArgs#h_args.geom,
    Opts = [
        {filter, HQArgs#h_args.filter},
        {nearest, HQArgs#h_args.nearest},
        {req_srid, HQArgs#h_args.req_srid},
        {resp_srid, HQArgs#h_args.resp_srid},
        {limit, HQArgs#h_args.limit + HQArgs#h_args.skip},
        {include_geom, HQArgs#h_args.include_geoms},
        {bookmark, HQArgs#h_args.bookmark}
    ],
    Resp = try
        easton_index:search(Idx#h_idx.pid, Shape, Opts)
    catch throw:Error ->
        Error
    end,
    {reply, Resp, St};

handle_call(info, _From, St) ->
    Idx = St#st.index,
    {ok, Info} = easton_index:info(Idx#h_idx.pid),
    {reply, {ok, Info}, St};

handle_call({new_seq, Seq}, _From, St) ->
    Idx = St#st.index,
    ok = easton_index:put(Idx#h_idx.pid, update_seq, Seq),
    {reply, ok, St};

handle_call({update, Id, Geoms}, _From, St) ->
    Idx = St#st.index,
    Resp = (catch easton_index:update(Idx#h_idx.pid, Id, Geoms)),
    {reply, Resp, St};

handle_call({remove, Id}, _From, St) ->
    Idx = St#st.index,
    Resp = (catch easton_index:remove(Idx#h_idx.pid, Id)),
    {reply, Resp, St}.


handle_cast(_Msg, St) ->
    {noreply, St}.


handle_info({'EXIT', Pid, {updated, NewSeq}}, #st{updater_pid = Pid} = St) ->
    Index0 = St#st.index,
    Index = Index0#h_idx{update_seq = NewSeq},
    NewSt = case reply_with_index(Index, St#st.waiting_list) of
        [] ->
            St#st{
                index = Index,
                updater_pid = undefined,
                waiting_list = []
            };
        StillWaiting ->
            Pid = spawn_link(hastings_index_updater, update, [self(), Index]),
            St#st{
                index = Index,
                updater_pid = Pid,
                waiting_list = StillWaiting
            }
    end,
    {noreply, NewSt};
handle_info({'EXIT', Pid, Reason}, #st{updater_pid=Pid} = St) ->
    Fmt = "~s ~s closing: Updater pid ~p closing w/ reason ~w",
    ?LOG_INFO(Fmt, [?MODULE, index_name(St#st.index), Pid, Reason]),
    [gen_server:reply(P, {error, Reason}) || {P, _} <- St#st.waiting_list],
    {stop, normal, St};
handle_info({'EXIT', Pid, Reason}, #st{index=#h_idx{pid={Pid}}} = St) ->
    Fmt = "Index for ~s closed with reason ~w",
    ?LOG_INFO(Fmt, [index_name(St#st.index), Reason]),
    [gen_server:reply(P, {error, Reason}) || {P, _} <- St#st.waiting_list],
    {stop, normal, St};
handle_info({'DOWN', _ , _, Pid, Reason}, St) ->
    Fmt = "~s ~s closing: Db pid ~p closing w/ reason ~w",
    ?LOG_INFO(Fmt, [?MODULE, index_name(St#st.index), Pid, Reason]),
    [gen_server:reply(P, {error, Reason}) || {P, _} <- St#st.waiting_list],
    {stop, normal, St}.


code_change(_OldVsn, St, _Extra) ->
    {ok, St}.


open_index(DbName, Idx) ->
    IdxDir = index_directory(DbName, Idx#h_idx.sig),
    Opts = [
        {type, Idx#h_idx.type},
        {dimensions, Idx#h_idx.dimensions},
        {srid, Idx#h_idx.srid}
    ],
    case easton_index:open(IdxDir, Opts) of
        {ok, Pid} ->
            UpdateSeq = easton_index:get(Pid, update_seq, 0),
            {ok, Idx#h_idx{
                pid = Pid,
                dbname = DbName,
                update_seq = UpdateSeq
            }};
        Error ->
            Error
    end.


reply_with_index(Index, WaitList) ->
    lists:foldl(fun({From, ReqSeq} = W, Acc) ->
        case ReqSeq =< Index#h_idx.update_seq of
            true ->
                gen_server:reply(From, ok),
                Acc;
            false ->
                [W | Acc]
        end
    end, [], WaitList).


design_doc_to_index_int(Id, Fields, IndexName) ->
    {RawIndexes} = case couch_util:get_value(<<"geo_indexes">>, Fields) of
        undefined ->
            {[]};
        {_} = RI ->
            RI;
        Else ->
            throw({invalid_index_object, Else})
    end,
    case lists:keyfind(IndexName, 1, RawIndexes) of
        false ->
            throw({not_found, <<IndexName/binary, " not found.">>});
        {IndexName, {IdxProps}} ->
            Idx = #h_idx{
                ddoc_id = Id,
                name = IndexName,
                def = get_index_def(IdxProps),
                lang = get_index_lang(Fields),

                type = get_index_type(IdxProps),
                dimensions = get_index_dimensions(IdxProps),
                srid = get_index_srid(IdxProps)
            },
            {ok, set_index_sig(Idx)};
        _ ->
            throw({invalid_index, <<IndexName/binary, " is invalid.">>})
    end.


set_index_sig(Idx) ->
    SigTerm = {
        Idx#h_idx.def,
        Idx#h_idx.type,
        Idx#h_idx.dimensions,
        Idx#h_idx.srid
    },
    Sig = ?l2b(couch_util:to_hex(couch_util:md5(term_to_binary(SigTerm)))),
    Idx#h_idx{sig = Sig}.


get_index_def(IdxProps) ->
    case couch_util:get_value(<<"index">>, IdxProps) of
        Bin when is_binary(Bin) ->
            Bin;
        undefined ->
            throw({invalid_index_definition, not_found});
        Else ->
            throw({invalid_index_definition, Else})
    end.


get_index_lang(Fields) ->
    Lang = couch_util:get_value(<<"language">>, Fields, <<"javascript">>),
    if is_binary(Lang) -> ok; true ->
        throw({invalid_index_language, Lang})
    end,
    Lang.


get_index_type(IdxProps) ->
    case couch_util:get_value(<<"type">>, IdxProps, <<"rtree">>) of
        <<"rtree">> -> <<"rtree">>;
        <<"tprtree">> -> <<"tprtree">>;
        Else -> throw({invalid_index_type, Else})
    end.


get_index_dimensions(IdxProps) ->
    case couch_util:get_value(<<"dimensions">>, IdxProps, 2) of
        N when N == 2; N == 3; N == 4 ->
            N;
        Else ->
            throw({invalid_index_dimensions, Else})
    end.


get_index_srid(IdxProps) ->
    case couch_util:get_value(<<"srid">>, IdxProps, 4326) of
        N when is_integer(N), N > 0 ->
            N;
        B when is_binary(B) ->
            parse_srid(B);
        Else ->
            throw({invalid_index_srid, Else})
    end.


parse_srid(<<"urn:ogc:def:crs:EPSG::", Tail/binary>> = Val) ->
    try
        SRID = integer_to_list(binary_to_list(Tail)),
        if SRID > 0 -> ok; true ->
            throw(bad_srid)
        end,
        SRID
    catch _:_ ->
        throw({error, {invalid_srid, Val}})
    end.


index_name(#h_idx{dbname=DbName, ddoc_id=DDocId, name=IndexName}) ->
    <<DbName/binary, " ", DDocId/binary, " ", IndexName/binary>>.


index_directory(DbName, Sig) ->
    GeoDir = config:get("hastings", "geo_index_dir", "/srv/geo_index"),
    filename:join([GeoDir, DbName, Sig]).
