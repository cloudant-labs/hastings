%% -*- erlang-indent-level: 4;indent-tabs-mode: nil -*-
%% Copyright 2012 Cloudant

-module(hastings_httpd).

-export([handle_search_req/3, handle_info_req/3, handle_cleanup_req/2]).
-include("hastings.hrl").
-include_lib("couch/include/couch_db.hrl").
-import(chttpd, [send_method_not_allowed/2, send_json/2, send_json/3,
                 send_response/4, send_error/2]).

handle_search_req(#httpd{method='GET', path_parts=[_, _, _, _, IndexName]}=Req
                  ,#db{name=DbName}, DDoc) ->
    % TODO change this to stream response as the hits return
    % match query args against open search / leaflet string
    QueryArgs = #index_query_args{
        bbox = BBox,
        wkt=Wkt,
        radius=Radius,
        lat=Lat,
        lon=Lon,
        include_docs = IncludeDocs
    } = parse_index_params(Req),
    % check we have at least one of radius, wkt or bbox
    case (BBox == undefined) and (Wkt == undefined) and 
        ((Radius == undefined) and (Lat == undefined) and (Lon == undefined)) of
    true ->
        Msg = <<"must include a query argument argument">>,
        throw({query_parse_error, Msg});
    _ ->
        ok
    end,

    % look at geojson spec to see how to add TotalHits
    case hastings_fabric_search:go(DbName, DDoc, IndexName, QueryArgs) of
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
        send_error(Req, Reason)
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
validate_index_query(radius, Value, Args) ->
    Args#index_query_args{radius=Value};
validate_index_query(lat, Value, Args) ->
    Args#index_query_args{lat=Value};
validate_index_query(lon, Value, Args) ->
    Args#index_query_args{lon=Value};
validate_index_query(stale, Value, Args) ->
    Args#index_query_args{stale=Value};
validate_index_query(limit, Value, Args) ->
    Args#index_query_args{limit=Value};
validate_index_query(include_docs, Value, Args) ->
    Args#index_query_args{include_docs=Value};
validate_index_query(extra, _Value, Args) ->
    Args.

parse_index_param("", _) ->
    [];
parse_index_param("bbox", Value) ->
    [MinX, MinY, MaxX, MaxY] = string:tokens(Value, ","),
    [{bbox, [parse_float_param(MinX),
             parse_float_param(MinY),
             parse_float_param(MaxX),
             parse_float_param(MaxY)]}];
parse_index_param("g", Value) ->
    [{g, Value}];    
parse_index_param("radius", Value) ->
    [{radius, parse_float_param(Value)}];
parse_index_param("lat", Value) ->
    [{lat, parse_float_param(Value)}];
parse_index_param("lon", Value) ->
    [{lon, parse_float_param(Value)}];
parse_index_param("limit", Value) ->
    [{limit, parse_positive_int_param(Value)}];
parse_index_param("stale", "ok") ->
    [{stale, ok}];
parse_index_param("stale", _Value) ->
    throw({query_parse_error, <<"stale only available as stale=ok">>});
parse_index_param("include_docs", Value) ->
    [{include_docs, parse_bool_param(Value)}];
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
        couch_config:get("hastings", "max_limit", "200")),
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
