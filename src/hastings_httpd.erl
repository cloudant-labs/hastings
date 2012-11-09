%% -*- erlang-indent-level: 4;indent-tabs-mode: nil -*-
%% Copyright 2012 Cloudant

-module(hastings_httpd).

-export([handle_search_req/3, handle_info_req/3, handle_cleanup_req/2]).
-include("hastings.hrl").
-include_lib("couch/include/couch_db.hrl").
-import(chttpd, [send_method_not_allowed/2, send_json/2, send_json/3, send_error/2]).

handle_search_req(#httpd{method='GET', path_parts=[_, _, _, _, IndexName]}=Req
                  ,#db{name=DbName}, DDoc) ->
    QueryArgs = #index_query_args{
        q = Query,
        include_docs = _IncludeDocs
    } = parse_index_params(Req),
    case Query of
        undefined ->
            Msg = <<"Query must include a 'q' or 'query' argument">>,
            throw({query_parse_error, Msg});
        _ ->
            ok
    end,
    case hastings_fabric_search:go(DbName, DDoc, IndexName, QueryArgs) of
    {ok, Bookmark0, TotalHits, _Hits0} ->
        Hits = [todo],
        Bookmark = hastings_fabric_search:pack_bookmark(Bookmark0),
        send_json(Req, 200, {[
            {total_rows, TotalHits},
            {bookmark, Bookmark},
            {rows, Hits}
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

validate_index_query(q, Value, Args) ->
    Args#index_query_args{q=Value};
validate_index_query(stale, Value, Args) ->
    Args#index_query_args{stale=Value};
validate_index_query(limit, Value, Args) ->
    Args#index_query_args{limit=Value};
validate_index_query(include_docs, Value, Args) ->
    Args#index_query_args{include_docs=Value};
validate_index_query(bookmark, Value, Args) ->
    Args#index_query_args{bookmark=Value};
validate_index_query(sort, Value, Args) ->
    Args#index_query_args{sort=Value};
validate_index_query(extra, _Value, Args) ->
    Args.

parse_index_param("", _) ->
    [];
parse_index_param("q", Value) ->
    [{q, ?l2b(Value)}];
parse_index_param("query", Value) ->
    [{q, ?l2b(Value)}];
parse_index_param("bookmark", Value) ->
    [{bookmark, ?l2b(Value)}];
parse_index_param("sort", Value) ->
    [{sort, ?JSON_DECODE(Value)}];
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
