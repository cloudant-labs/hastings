-module(hastings_format_view).

-include("hastings.hrl").

-export([hits_to_json/3]).

hits_to_json(DbName, Hits, HQArgs) ->
    Bookmark = hastings_bookmark:update(HQArgs#h_args.bookmark, Hits),
    BookmarkJson = hastings_bookmark:pack(Bookmark),
    {[], hits_to_json0(DbName, Hits, HQArgs, BookmarkJson)}.


hits_to_json0(DbName, Hits, HQArgs, Bookmark) ->
    Hits0 = maybe_add_docs_in_view_format(DbName, Hits, HQArgs),
    Docs = lists:map(fun(H) ->
        Geom = case H#h_hit.geom of
            undefined -> null;
            Geom0 -> Geom0
        end,
        case HQArgs#h_args.include_docs of
            false ->
                {_, Rev} = lists:keyfind(<<"_rev">>, 1,H#h_hit.doc),
                {[
                    {<<"id">>, H#h_hit.id},
                    {<<"rev">>, Rev},
                    {<<"geometry">>, Geom}
                ]};
            true ->
                {[
                    {<<"id">>, H#h_hit.id},
                    {<<"geometry">>, Geom},
                    {doc, H#h_hit.doc}
                ]}
        end
    end, Hits0),
    {[
        {<<"bookmark">>, Bookmark},
        {<<"rows">>, Docs}
    ]}.


maybe_add_docs_in_view_format(DbName, Hits, #h_args{include_docs=false}) ->
    add_docs_in_view_format(DbName, Hits);
maybe_add_docs_in_view_format(_DbName, Hits, _) ->
    Hits.


add_docs_in_view_format(DbName, Hits) ->
    DocIds = [Id || #h_hit{id=Id} <- Hits],
    {ok, Docs} = hastings_util:get_json_docs(DbName, DocIds),
    lists:map(fun(H) ->
        {_, {Doc}} = lists:keyfind(H#h_hit.id, 1, Docs),
        H#h_hit{doc = Doc}
    end, Hits).
