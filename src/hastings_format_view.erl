-module(hastings_format_view).

-include("hastings.hrl").

-export([hits_to_json/2]).

hits_to_json(Hits, HQArgs) ->
    Bookmark = hastings_bookmark:update(HQArgs#h_args.bookmark, Hits),
    BookmarkJson = hastings_bookmark:pack(Bookmark),
    {[], hits_to_json0(Hits, BookmarkJson)}.


hits_to_json0(Hits, Bookmark) ->
    Docs = lists:map(fun(H) ->
        Geom = case H#h_hit.geom of
            undefined -> null;
            Geom0 -> Geom0
        end,
        case H#h_hit.doc of
            undefined ->
                {[
                    {<<"id">>, H#h_hit.id},
                    {<<"geometry">>, Geom}
                ]};
            Doc ->
                {[
                    {<<"id">>, H#h_hit.id},
                    {<<"geometry">>, Geom},
                    {doc, Doc}
                ]}
        end
    end, Hits),
    {[
        {<<"bookmark">>, Bookmark},
        {<<"rows">>, Docs}
    ]}.