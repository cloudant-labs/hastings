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


hits_to_json(Hits, HQArgs) ->
    Bookmark = hastings_bookmark:update(HQArgs#h_args.bookmark, Hits),
    BookmarkJson = hastings_bookmark:pack(Bookmark),
    hits_to_json0(Hits, BookmarkJson).


hits_to_json0(Hits, Bookmark) ->
    Geoms = lists:map(fun(H) ->
        Dist = [{<<"distance">>, H#h_hit.dist}],
        Doc = case H#h_hit.doc of
            undefined -> [];
            Doc0 -> [{doc, Doc0}]
        end,
        Properties = {Dist ++ Doc},
        Geom = case H#h_hit.geom of
            undefined -> null;
            Geom0 -> Geom0
        end,
        {[
            {<<"id">>, H#h_hit.id},
            {<<"geometry">>, Geom},
            {<<"properties">>, Properties}
        ]}
    end, Hits),
    {[
        {<<"bookmark">>, Bookmark},
        {<<"type">>, <<"FeatureCollection">>},
        {<<"features">>, Geoms}
    ]}.


parse_search_req(Req) ->
    Params = hastings_httpd_util:parse_query(Req, search_parameters()),
    set_record_fields(#h_args{geom = get_geometry(Params)}, Params).


get_geometry(Params) ->
    WKT = lists:keyfind(wkt, 1, Params),
    BBox = lists:keyfind(bbox, 1, Params),
    Circle = get_geometry(circle, [x, y, r], Params),
    Ellipse = get_geometry(ellipse, [x, y, x_range, y_range], Params),
    Filt = fun(false) -> false; (_) -> true end,
    Geoms = lists:filter(Filt, [WKT, BBox, Circle, Ellipse]),
    case Geoms of
        [] ->
            throw({query_parse_error, "No geometry query specified."});
        [Geom] ->
            Geom;
        [_|_] ->
            throw({query_parse_error, "Multiple geometry queries specified."})
    end.


get_geometry(Name, Params, AllParams) ->
    Values = [lists:keyfind(P, 1, AllParams) || P <- Params],
    case lists:member(false, Values) of
        true ->
            false;
        false ->
            {Name, list_to_tuple([element(2, P) || P <- Values])}
    end.


set_record_fields(HQArgs0, Params) ->
    HQArgs = lists:foldl(fun({Key, Val}, ArgAcc) ->
        Idx = case Key of
            nearest ->          #h_args.nearest;
            filter ->           #h_args.filter;
            req_srid ->         #h_args.req_srid;
            resp_srid ->        #h_args.resp_srid;
            vbox ->             #h_args.vbox;
            t_start ->          #h_args.t_start;
            t_end ->            #h_args.t_end;
            limit ->            #h_args.limit;
            skip ->             #h_args.skip;
            stale ->            #h_args.stale;
            stable ->           #h_args.stable;
            include_docs ->     #h_args.include_docs;
            include_geoms ->    #h_args.include_geoms;
            bookmark ->         #h_args.bookmark;
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
    end, HQArgs0, Params),
    % Check for our geometry/filter combinations for
    % acceptability and to set the default relation.
    Default = case HQArgs#h_args.geom of
        {bbox, Coords} when length(Coords) > 4 ->
            % If a user specifies a bonding box with more
            % than two dimension we need to make sure they
            % aren't trying to use a filter as that would
            % return meaningless results due to how libgeos
            % works.
            case HQArgs#h_args.filter of
                undefined ->
                    none;
                none ->
                    none;
                _ ->
                    throw({query_parse_error, filter_with_md_bbox})
            end;
        {bbox, _Coords} ->
            % All bbox queries default to a basic MBR
            % query. Fancy relationships need to be
            % requested specifically.
            none;
        _ when not HQArgs#h_args.nearest ->
            intersects;
        _ ->
            none
    end,
    Filter = case HQArgs#h_args.filter of
        undefined -> Default;
        Else -> Else
    end,
    HQArgs#h_args{filter = Filter}.


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
        {<<"srid">>,            req_srid,       to_string},
        {<<"response_srid">>,   resp_srid,      to_string},

        % Temporal parameters
        {<<"vbox">>,            vbox,           to_bbox},
        {<<"start">>,           t_start,        to_float},
        {<<"end">>,             t_end,          to_float},

        % Result set parameters
        {<<"limit">>,           limit,          to_limit},
        {<<"skip">>,            skip,           to_pos_int},
        {<<"stale">>,           stale,          to_stale},
        {<<"stable">>,          stable,         to_bool},
        {<<"include_docs">>,    include_docs,   to_bool},
        {<<"include_geoms">>,   include_geoms,  to_bool},
        {<<"bookmark">>,        bookmark,       to_bookmark},

        % Backwards compatibility
        {<<"lon">>,             x,              to_float},
        {<<"lat">>,             y,              to_float},
        {<<"radius">>,          r,              to_float},
        {<<"rangex">>,          x_range,        to_float},
        {<<"rangey">>,          y_range,        to_float},
        {<<"relation">>,        filter,         to_filter},
        {<<"startIndex">>,      skip,           to_pos_int},
        {<<"srs">>,             req_srid,       to_string},
        {<<"responseSrs">>,     resp_srid,      to_string}
    ].
