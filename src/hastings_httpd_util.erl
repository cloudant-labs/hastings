%% Copyright 2014 Cloudant

-module(hastings_httpd_util).


-export([
    parse_query/2
]).

-export([
    to_bool/2,
    to_pos_int/2,
    to_float/2,
    to_string/2,
    to_bookmark/2,
    to_format/2,
    to_bbox/2,
    to_filter/2,
    to_limit/2,
    to_stale/2
]).


-include_lib("couch/include/couch_db.hrl").


parse_query(#httpd{}=Req, ParamDescrs) ->
    StrParams = chttpd:qs(Req),
    BinParams = [
        {list_to_binary(K), list_to_binary(V)} || {K, V} <- StrParams
    ],
    parse_query({BinParams}, ParamDescrs);
parse_query({Props}, ParamDescrs) ->
    lists:flatmap(fun({Key, Val}) ->
        parse_param(Key, Val, ParamDescrs)
    end, Props).


parse_param(<<>>, _, _) ->
    [];
parse_param(Key, Val, Descs) ->
    case lists:keyfind(Key, 1, Descs) of
        false ->
            [{extra, [{Key, Val}]}];
        {Key, PName, Fun} ->
            [{PName, ?MODULE:Fun(Key, Val)}]
    end.


to_bool(_Name, Value) when is_boolean(Value) ->
    Value;
to_bool(_Name, Value) when is_number(Value), Value /= 0 ->
    true;
to_bool(_Name, Value) when is_number(Value), Value == 0 ->
    false;
to_bool(_Name, <<"true">>) ->
    true;
to_bool(_Name, <<"false">>) ->
    false;
to_bool(Name, Value) ->
    invalid_value("boolean", Name, Value).


to_pos_int(_Name, Value) when is_integer(Value), Value >= 0 ->
    Value;
to_pos_int(Name, Value) when is_binary(Value) ->
    ValStr = binary_to_list(Value),
    ValInt = try
        list_to_integer(ValStr)
    catch _:_ ->
        -1
    end,
    if ValInt >= 0 -> ok; true ->
        invalid_value("positive integer", Name, Value)
    end,
    ValInt;
to_pos_int(Name, Value) ->
    invalid_value("positive integer", Name, Value).


to_float(_Name, Value) when is_number(Value) ->
    float(Value);
to_float(Name, Value) when is_binary(Value) ->
    StrVal = binary_to_list(Value),
    FloatVal = case string:to_float(StrVal) of
        {R, []} ->
            R;
        _ ->
            % Numbers without a decimal point aren't
            % parsed by string:to_float/1 so we attempt
            % to read an integer for user convenience.
            try
                float(list_to_integer(StrVal))
            catch _:_ ->
                invalid_value("float", Name, Value)
            end
    end,
    FloatVal;
to_float(Name, Value) ->
    invalid_value("float", Name, Value).


to_string(_Name, Value) when is_binary(Value) ->
    Value;
to_string(Name, Value) ->
    invalid_value("string", Name, Value).


to_bookmark(Name, Value) when is_binary(Value) ->
    try
        hastings_bookmark:unpack(Value)
    catch _:_ ->
        invalid_value("bookmark", Name, Value)
    end;
to_bookmark(Name, Value) ->
    invalid_value("bookmark", Name, Value).


to_format(_Name, null) ->
    hastings_format_view;
to_format(_Name, <<"view">>) ->
    hastings_format_view;
to_format(_Name, <<"geojson">>) ->
    hastings_format_geojson;
to_format(_Name, <<"application/vnd.geo+json">>) ->
    hastings_format_geojson;
to_format(_Name, BadValue) -> 
    invalid_value("format name", "format", BadValue).


to_bbox(Name, Value) when is_binary(Value) ->
    TokenStrs = string:tokens(binary_to_list(Value), ","),
    Tokens = [list_to_binary(TS) || TS <- TokenStrs],
    NumToks = length(Tokens),
    if NumToks == 4; NumToks == 6; NumToks == 8 -> ok; true ->
        invalid_value("bounding box", Name, Value)
    end,
    Coords = lists:map(fun(S) ->
        to_float(Name, S)
    end, Tokens),
    Coords;
to_bbox(Name, Value) ->
    invalid_value("bounding box", Name, Value).


to_filter(_Name, null) ->
    <<"none">>;
to_filter(Name, Value) when is_binary(Value) ->
    case lists:member(Value, filter_names()) of
        true ->
            Value;
        false ->
            invalid_value("filter name", Name, Value)
    end,
    Value;
to_filter(Name, Value) ->
    invalid_value("filter name", Name, Value).


to_limit(_Name, Value) when is_integer(Value), Value >= 0 ->
    MaxVal = try
        StrMax = config:get("hastings", "max_limit", "200"),
        list_to_integer(StrMax)
    catch _:_ ->
        200
    end,
    if Value =< MaxVal -> ok; true ->
        Fmt = "Limit must be less than or equal to ~b",
        Msg = iolist_to_binary(io_lib:format(Fmt, [MaxVal])),
        throw({query_parse_error, Msg})
    end,
    Value;
to_limit(Name, Value) ->
    IntVal = to_pos_int("limit", Value),
    to_limit(Name, IntVal).


to_stale(_Name, <<"ok">>) ->
    true;
to_stale(_Name, <<"update_after">>) ->
    update_after;
to_stale(Name, Value) ->
    to_bool(Name, Value).


invalid_value(Type, Name, Value) ->
    ValStr = try
        ?JSON_ENCODE(Value)
    catch _:_ ->
        lists:flatten(io_lib:format("~w", [Value]))
    end,
    Fmt = "Invalid ~s value for ~s: '~s'",
    Msg = iolist_to_binary(io_lib:format(Fmt, [Type, Name, ValStr])),
    throw({query_parse_error, Msg}).


filter_names() ->
    [
        <<"none">>,
        <<"contains">>,
        <<"contains_properly">>,
        <<"covered_by">>,
        <<"covers">>,
        <<"crosses">>,
        <<"disjoint">>,
        <<"intersects">>,
        <<"overlaps">>,
        <<"touches">>,
        <<"within">>
    ].
