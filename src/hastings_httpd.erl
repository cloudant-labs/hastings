%% Copyright 2014 Cloudant

-module(hastings_httpd).


-include("hastings.hrl").
-include_lib("couch/include/couch_db.hrl").


-export([
    handle_search_req/3,
    handle_info_req/3,
    handle_cleanup_req/2
]).


handle_search_req(#httpd{method='GET', path_parts=PP}=Req, Db, DDoc)
        when length(PP) == 5 ->
    DbName = couch_db:name(Db),
    IndexName = lists:nth(5, PP),
    HQArgs = parse_search_req(Req),
    case hastings_fabric_search:go(DbName, DDoc, IndexName, HQArgs) of
        {ok, Hits} ->
            ResultJson = hits_to_json(Hits, HQArgs),
            chttpd:send_json(Req, 200, ResultJson);
        {error, Reason} ->
            chttpd:send_error(Req, Reason);
        {timeout, _} ->
            chttpd:send_error(Req, <<"Timeout in server request">>)
    end;
handle_search_req(Req, _Db, _DDoc) ->
    chttpd:send_method_not_allowed(Req, "GET").


handle_info_req(#httpd{method='GET', path_parts=PP}=Req, Db, DDoc)
        when length(PP) == 5 ->
    DbName = couch_db:name(Db),
    DDocId = DDoc#doc.id,
    IndexName = lists:last(PP),
    case hastings_fabric_info:go(DbName, DDoc, IndexName) of
        {ok, IndexInfoList} ->
            chttpd:send_json(Req, 200, {[
                {name,  <<DDocId/binary,"/",IndexName/binary>>},
                {geo_index, {IndexInfoList}}
            ]});
        {error, Reason} ->
            chttpd:send_error(Req, Reason)
    end;
handle_info_req(Req, _Db, _DDoc) ->
    chttpd:send_method_not_allowed(Req, "GET").


handle_cleanup_req(#httpd{method='POST'}=Req, Db) ->
    ok = hastings_vacuum:cleanup(couch_db:name(Db)),
    chttpd:send_json(Req, 202, {[{ok, true}]});
handle_cleanup_req(Req, _Db) ->
    chttpd:send_method_not_allowed(Req, "POST").


hits_to_json(Hits, #h_args{include_geoms=true}) ->
    Geoms = lists:map(fun(H) ->
        {GeomProps} = H#h_hit.geom,
        IdProp = [{id, H#h_hit.id}],
        DocProp = case H#h_hit.doc of
            undefined -> [];
            Doc -> [{doc, Doc}]
        end,
        {IdProp ++ GeomProps ++ DocProp}
    end, Hits),
    {[
        {<<"type">>, <<"GeometryCollection">>},
        {<<"geometries">>, Geoms}
    ]};
hits_to_json(Hits, _) ->
    Features = lists:map(fun(H) ->
        IdProp = [{id, H#h_hit.id}],
        DocProp = case H#h_hit.doc of
            undefined -> [];
            Doc -> [{doc, Doc}]
        end,
        {IdProp ++ [{<<"type">>, <<"Feature">>}] ++ DocProp}
    end, Hits),
    {[
        {<<"type">>, <<"FeatureCollection">>},
        {<<"features">>, Features}
    ]}.


parse_search_req(Req) ->
    Params = hastings_httpd_util:parse_query(Req, search_parameters()),
    set_record_fields(#h_args{geom = get_geom(Params)}, Params).


get_geom(Params) ->
    WKT = lists:keyfind(wkt, 1, Params),
    BBox = lists:keyfind(bbox, 1, Params),
    Circle = get_shape(circle, [x, y, r], Params),
    Ellipse = get_shape(ellipse, [x, y, x_range, y_range], Params),
    Filt = fun(false) -> false; (_) -> true end,
    Shapes = lists:filter(Filt, [WKT, BBox, Circle, Ellipse]),
    case Shapes of
        [] ->
            throw({query_parse_error, "No geometry query specified."});
        [Shape] ->
            Shape;
        [_|_] ->
            throw({query_parse_error, "Multiple geometry queries specified."})
    end.


get_shape(Name, Params, AllParams) ->
    Values = [lists:keyfind(P, 1, AllParams) || P <- Params],
    case lists:member(false, Values) of
        true ->
            false;
        false ->
            {Name, list_to_tuple([element(2, P) || P <- Values])}
    end.


set_record_fields(HQArgs, Params) ->
    lists:foldl(fun({Key, Val}, ArgAcc) ->
        Idx = case Key of
            nearest ->          #h_args.nearest;
            filter ->           #h_args.filter;
            req_srid ->         #h_args.req_srid;
            resp_srid ->        #h_args.resp_srid;
            t_start ->          #h_args.t_start;
            t_end ->            #h_args.t_end;
            limit ->            #h_args.limit;
            skip ->             #h_args.skip;
            stale ->            #h_args.stale;
            include_docs ->     #h_args.include_docs;
            include_geoms ->    #h_args.include_geoms;
            extra ->            extra;
            _ ->                ignore
        end,
        case Idx of
            I when is_integer(I) ->
                setelement(Idx, ArgAcc, Val);
            extra ->
                E = ArgAcc#h_args.extra,
                ArgAcc#h_args{extra = [Val | E]};
            ignore ->
                ArgAcc
        end
    end, HQArgs, Params).


search_parameters() ->
    [
        % Query/Geometry parameters
        {<<"g">>,               wkt,            to_string},
        {<<"geometry">>,        wkt,            to_string},
        {<<"bbox">>,            bbox,           to_bbox},
        {<<"x">>,               x,              to_float},
        {<<"y">>,               y,              to_float},
        {<<"r">>,               r,              to_float},
        {<<"x_range">>,         x_range,        to_float},
        {<<"y_range">>,         y_range,        to_float},

        % Geo parameters
        {<<"nearest">>,         nearest,        to_bool},
        {<<"filter">>,          filter,         to_filter},
        {<<"srid">>,            req_srid,       to_pos_int},
        {<<"response_srid">>,   resp_srid,      to_pos_int},

        % Temporal parameters
        {<<"start">>,           t_start,        to_float},
        {<<"end">>,             t_end,          to_float},

        % Result set parameters
        {<<"limit">>,           limit,          to_limit},
        {<<"skip">>,            skip,           to_pos_int},
        {<<"stale">>,           stale,          to_stale},
        {<<"include_docs">>,    include_docs,   to_bool},
        {<<"include_geoms">>,   include_geoms,  to_bool},

        % Backwards compatibility
        {<<"lon">>,             x,              to_float},
        {<<"lat">>,             y,              to_float},
        {<<"radius">>,          r,              to_float},
        {<<"rangex">>,          x_range,        to_float},
        {<<"rangey">>,          y_range,        to_float},
        {<<"relation">>,        filter,         to_filter},
        {<<"startIndex">>,      skip,           to_pos_int},
        {<<"srs">>,             req_srid,       to_pos_int},
        {<<"responseSrs">>,     resp_srid,      to_pos_int}
    ].
