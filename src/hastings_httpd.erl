%% Copyright 2014 Cloudant

-module(hastings_httpd).


-include("hastings.hrl").
-include_lib("couch/include/couch_db.hrl").


-import(chttpd, [
    send_method_not_allowed/2,
    send_json/2,
    send_json/3,
    send_response/4,
    send_error/2
]).


-export([
    handle_search_req/3,
    handle_info_req/3,
    handle_cleanup_req/2
]).


handle_search_req(#httpd{method='GET', path_parts=[_, _, _, _, IndexName]}=Req,
        #db{name=DbName}, DDoc) ->
    % match query args against open search / leaflet string
    QueryArgs = #index_query_args{
        bbox = BBox,
        wkt=Wkt,
        radius=Radius,
        range_x=EllipseX,
        range_y=EllipseY,
        x=X,
        y=Y,
        include_docs = IncludeDocs
    } = parse_index_params(Req),
    % check we have at least one of radius, wkt, bbox or ellipse
    case (BBox == undefined )
        and
        (Wkt == undefined)
        and
        (
            (Radius == undefined) and (X == undefined) and (Y == undefined)
        )
        and
        (
            (EllipseX == undefined) and (EllipseY == undefined)
            and (X == undefined) and (Y == undefined)
        ) of
    true ->
        Msg = <<"must include a query argument argument">>,
        throw({query_parse_error, Msg});
    _ ->
        ok
    end,

    % TODO look at geojson spec to see how to add TotalHits
    case page_results(DbName, DDoc, IndexName, QueryArgs, 0, 0, []) of
    {ok, _TotalHits, Hits0} ->
        Hits = [
            begin
                case IncludeDocs of
                true ->
                    Body = case fabric:open_doc(DbName, Id, []) of
                    {ok, Doc} ->
                        {Resp} = Doc#doc.body,
                        Resp;
                    {not_found, _} -> null
                    end,
                    case lists:keyfind(<<"type">>, 1, Body) of
                    false ->
                        {[{type, <<"Feature">>}, {id, Id} | Body]};
                    _ ->
                        {[{id, Id} | Body]}
                    end;
                false ->
                    {[{id, Id}]}
                end
            end || {Id, _} <- Hits0
        ],
        send_json(Req, 200, {[
            {type, <<"FeatureCollection">>},
            {features, Hits}
        ]});
    {error, Reason} ->
        send_error(Req, Reason);
    {timeout, _} ->
        send_error(Req, <<"Timeout in server request">>)
    end;

handle_search_req(Req, _Db, _DDoc) ->
    send_method_not_allowed(Req, "GET").

handle_info_req(#httpd{method='GET', path_parts=[_, _, _, _, IndexName]}=Req
                  ,#db{name=DbName}, #doc{id=Id}=DDoc) ->
    case hastings_fabric_info:go(DbName, DDoc, IndexName) of
        {ok, IndexInfoList} ->
            send_json(Req, 200, {[
                {name,  <<Id/binary,"/",IndexName/binary>>},
                {search_index, {IndexInfoList}}
            ]});
        {error, Reason} ->
            send_error(Req, Reason)
    end;
handle_info_req(Req, _Db, _DDoc) ->
    send_method_not_allowed(Req, "GET").

handle_cleanup_req(#httpd{method='POST'}=Req, #db{name=DbName}) ->
    ok = hastings_fabric_cleanup:go(DbName),
    send_json(Req, 202, {[{ok, true}]});
handle_cleanup_req(Req, _Db) ->
    send_method_not_allowed(Req, "POST").

parse_index_params(Req) when not is_list(Req) ->
    IndexParams = lists:flatmap(fun({K, V}) -> parse_index_param(K, V) end,
        chttpd:qs(Req)),
    parse_index_params(IndexParams);
parse_index_params(IndexParams) ->
    Args = #index_query_args{},
    lists:foldl(fun({K, V}, Args2) ->
        validate_index_query(K, V, Args2)
    end, Args, IndexParams).

validate_index_query(bbox, Value, Args) ->
    Args#index_query_args{bbox=Value};
validate_index_query(g, Value, Args) ->
    Args#index_query_args{wkt=Value};
validate_index_query(relation, Value, Args) ->
    Args#index_query_args{relation=Value};
validate_index_query(radius, Value, Args) ->
    Args#index_query_args{radius=Value};
validate_index_query(y, Value, Args) ->
    Args#index_query_args{y=Value};
validate_index_query(x, Value, Args) ->
    Args#index_query_args{x=Value};
validate_index_query(stale, Value, Args) ->
    Args#index_query_args{stale=Value};
validate_index_query(limit, Value, Args) ->
    Args#index_query_args{limit=Value};
validate_index_query(include_docs, Value, Args) ->
    Args#index_query_args{include_docs=Value};
validate_index_query(startIndex, Value, Args) ->
    Args#index_query_args{startIndex=Value};
validate_index_query(srs, Value, Args) ->
    Args#index_query_args{srs=Value};
validate_index_query(responseSrs, Value, Args) ->
    Args#index_query_args{responseSrs=Value};
validate_index_query(range_x, Value, Args)->
    Args#index_query_args{range_x=Value};
validate_index_query(range_y, Value, Args)->
    Args#index_query_args{range_y=Value};
validate_index_query(nearest, Value, Args)->
    Args#index_query_args{nearest=Value};
validate_index_query(tStart, Value, Args)->
    Args#index_query_args{tStart=Value};
validate_index_query(tEnd, Value, Args)->
    Args#index_query_args{tEnd=Value};
validate_index_query(extra, _Value, Args) ->
    Args.

parse_index_param("", _) ->
    [];
parse_index_param("bbox", Value) ->
    Vals = string:tokens(Value, ","),
    case length(Vals) rem 2 of
    0 ->
        [{bbox, [parse_float_param(V)|| V <- Vals]}];
    _ ->
        Msg = io_lib:format("Invalid value for bbox parameter: ~p", [Vals]),
        throw({query_parse_error, ?l2b(Msg)})
    end;
parse_index_param("g", Value) ->
    [{g, Value}];
parse_index_param("relation", Value) ->
    [{relation, Value}];
parse_index_param("radius", Value) ->
    [{radius, parse_float_param(Value)}];
parse_index_param("lat", Value) ->
    [{y, parse_float_param(Value)}];
parse_index_param("lon", Value) ->
    [{x, parse_float_param(Value)}];
parse_index_param("y", Value) ->
    [{y, parse_float_param(Value)}];
parse_index_param("x", Value) ->
    [{x, parse_float_param(Value)}];
parse_index_param("limit", Value) ->
    [{limit, parse_positive_int_param(Value)}];
parse_index_param("stale", "ok") ->
    [{stale, ok}];
parse_index_param("stale", _Value) ->
    throw({query_parse_error, <<"stale only available as stale=ok">>});
parse_index_param("include_docs", Value) ->
    [{include_docs, parse_bool_param(Value)}];
parse_index_param("startIndex", Value) ->
    [{startIndex, parse_positive_int_param(Value)}];
parse_index_param("srs", Value) ->
    [{srs, Value}];
parse_index_param("responseSrs", Value) ->
    [{responseSrs, Value}];
parse_index_param("rangex", Value) ->
    [{range_x, parse_float_param(Value)}];
parse_index_param("rangey", Value) ->
    [{range_y, parse_float_param(Value)}];
parse_index_param("nearest", Value) ->
    [{nearest, parse_bool_param(Value)}];
parse_index_param("start", Value) ->
    [{tStart, parse_float_param(Value)}];
parse_index_param("end", Value) ->
    [{tEnd, parse_float_param(Value)}];
parse_index_param(Key, Value) ->
    [{extra, {Key, Value}}].

%% VV copied from chttpd_view.erl
parse_bool_param("true") -> true;
parse_bool_param("false") -> false;
parse_bool_param(Val) ->
    Msg = io_lib:format("Invalid value for boolean parameter: ~p", [Val]),
    throw({query_parse_error, ?l2b(Msg)}).

parse_int_param(Val) ->
    case (catch list_to_integer(Val)) of
    IntVal when is_integer(IntVal) ->
        IntVal;
    _ ->
        Msg = io_lib:format("Invalid value for integer parameter: ~p", [Val]),
        throw({query_parse_error, ?l2b(Msg)})
    end.

parse_float_param(Val) ->
    case string:to_float(Val) of
    {error,no_float} -> list_to_integer(Val);
    {F,_Rest} -> F
    end.

parse_positive_int_param(Val) ->
    MaximumVal = list_to_integer(
        config:get("hastings", "max_limit", "200")),
    case parse_int_param(Val) of
    IntVal when IntVal > MaximumVal ->
        Fmt = "Value for limit is too large, must not exceed ~p",
        Msg = io_lib:format(Fmt, [MaximumVal]),
        throw({query_parse_error, ?l2b(Msg)});
    IntVal when IntVal >= 0 ->
        IntVal;
    _ ->
        Fmt = "Invalid value for positive integer parameter: ~p",
        Msg = io_lib:format(Fmt, [Val]),
        throw({query_parse_error, ?l2b(Msg)})
    end.

page_results(DbName, DDoc, IndexName, #index_query_args{
        limit=Limit,
        startIndex=StartIndex,
        currentPage=CurrentPage,
        nearest=Nearest
    } = QueryArgs, CurrentIndex, TotalHits, HitsAcc) ->
    case (CurrentIndex + TotalHits) > StartIndex of
    true ->
        % stop here, we have gone past the index point,
        % result limit should always be less than page limit
        % note not an error to go over list length with sublist
        Hits0 = case Nearest of
          true ->
            % TODO sort by nearest
            HitsAcc;
          _ ->
            lists:sublist(lists:keysort(1, HitsAcc), StartIndex + 1, Limit)
        end,
        {ok, length(Hits0), Hits0};
    _ ->
        case hastings_fabric_search:go(DbName, DDoc, IndexName, QueryArgs) of
            {ok, 0, _} ->
               {ok, 0, HitsAcc};
            {ok, TotalHits0, Hits0} ->
                NewQueryArgs = QueryArgs#index_query_args{
                    currentPage = CurrentPage + 1
                },
                page_results(
                        DbName,
                        DDoc,
                        IndexName,
                        NewQueryArgs,
                        CurrentIndex + TotalHits0,
                        TotalHits0,
                        HitsAcc ++ Hits0
                    );
            Error ->
                Error
        end
    end.
