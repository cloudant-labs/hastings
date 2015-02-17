-module(hastings_format_legacy).

-include("hastings.hrl").

-export([hits_to_json/2]).

hits_to_json(Hits, HQArgs) ->
    Bookmark = hastings_bookmark:update(HQArgs#h_args.bookmark, Hits),
    BookmarkJson = hastings_bookmark:pack(Bookmark),
    {[], hits_to_json0(Hits, BookmarkJson)}.


hits_to_json0(Hits, Bookmark) ->
    Docs = lists:map(fun(H) ->
        case H#h_hit.doc of
            undefined ->
                {[{<<"id">>, H#h_hit.id}]};
            {Props0} ->
                Props = filter_props(Props0),
                Extra = case lists:keyfind(<<"type">>, 1, Props) of
                    false ->
                        [{<<"type">>, <<"Feature">>}];
                    _ ->
                        []
                end,
                {[{<<"id">>, H#h_hit.id}] ++ Extra ++ Props}
        end
    end, Hits),
    {[
        {<<"type">>, <<"FeatureCollection">>},
        {<<"features">>, Docs},
        {<<"bookmark">>, Bookmark}
    ]}.


filter_props(Props) ->
    Keys = [<<"_id">>, <<"_rev">>],
    lists:foldl(fun(Key, Acc) ->
        lists:keydelete(Key, 1, Acc)
    end, Props, Keys).
    
