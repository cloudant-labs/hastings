%% Copyright 2014 Cloudant

-module(hastings_util).


-export([
    get_json_docs/2
]).


get_json_docs(DbName, DocIds) ->
    Opts = [
        {keys, DocIds},
        {include_docs, true}
    ],
    fabric:all_docs(DbName, fun callback/2, [], Opts).


callback({meta, _}, Acc) ->
    {ok, Acc};
callback({total_and_offset,_,_}, Acc) ->
    {ok, Acc};
callback({error, Reason}, _Acc) ->
    {error, Reason};
callback({row, {Props}}, Acc) ->
    callback({row, Props}, Acc);
callback({row, Props}, Acc) when is_list(Props) ->
    {id, Id} = lists:keyfind(id, 1, Props),
    {doc, Doc} = lists:keyfind(doc, 1, Props),
    {ok, [{Id, Doc} | Acc]};
callback(complete, Acc) ->
    {ok, lists:reverse(Acc)};
callback(timeout, _Acc) ->
    {error, timeout}.
