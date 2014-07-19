%% Copyright 2014 Cloudant

-module(hastings_fabric_info).


-include("hastings.hrl").
-include_lib("mem3/include/mem3.hrl").
-include_lib("couch/include/couch_db.hrl").


-export([go/3]).


go(DbName, DDocId, IndexName) when is_binary(DDocId) ->
    {ok, DDoc} = fabric:open_doc(DbName, <<"_design/", DDocId/binary>>, []),
    go(DbName, DDoc, IndexName);

go(DbName, DDoc, IndexName) ->
    StartFun = info,
    StartArgs = [fabric_util:doc_id_and_rev(DDoc), IndexName],
    case hastings_fabric:run(DbName, StartFun, StartArgs) of
        {ok, Resps} ->
            merge_resps(Resps);
        Else ->
            Else
    end.


merge_resps(Resps) ->
    Dict = lists:foldl(fun({ok, Info}, D0) ->
        lists:foldl(fun({K, V}, D1) ->
            orddict:append(K, V, D1)
        end, Info, D0)
    end, orddict:new(), Resps),
    orddict:fold(fun
        (disk_size, X, Acc) ->
            [{disk_size, lists:sum(X)} | Acc];
        (doc_count, X, Acc) ->
            [{doc_count, lists:sum(X)} | Acc];
        (_, _, Acc) ->
            Acc
    end, [], Dict).
