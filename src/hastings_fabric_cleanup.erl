%% Copyright 2014 Cloudant

-module(hastings_fabric_cleanup).


-include_lib("mem3/include/mem3.hrl").
-include_lib("couch/include/couch_db.hrl").
-include("hastings.hrl").


-export([
    go/1
]).


go(DbName) ->
    {ok, DesignDocs} = fabric:design_docs(DbName),
    ActiveSigs = lists:usort(lists:flatmap(fun active_sigs/1,
        [couch_doc:from_json_obj(DD) || DD <- DesignDocs])),

    cleanup(DbName, ActiveSigs),
    ok.


active_sigs(#doc{body={Fields}}=Doc) ->
    {RawIndexes} = couch_util:get_value(<<"indexes">>, Fields, {[]}),
    {IndexNames, _} = lists:unzip(RawIndexes),
    [begin
         {ok, Index} = hastings_index:design_doc_to_index(Doc, IndexName),
         Index#index.sig
     end || IndexName <- IndexNames].


cleanup(<<"shards", _/binary>>=ShardName, ActiveSigs) ->
    cleanup(mem3:dbname(ShardName), ActiveSigs);

cleanup(DbName, _ActiveSigs) ->
    % get all geo indexes for this database
    % leave active indexes
    try fabric:design_docs(DbName) of
        {ok, DesignDocs} ->
            {ok, DesignDocs} = fabric:design_docs(DbName),
        ActiveSigs = lists:map(fun(#doc{id = GroupId}) ->
             {ok, Info} = fabric:get_view_group_info(DbName, GroupId),
             binary_to_list(couch_util:get_value(signature, Info))
        end, [couch_doc:from_json_obj(DD) || DD <- DesignDocs]),


        % get all indexes for database
        % list all .geo and .geopriv
        FileList = filelib:wildcard([config:get("couchdb", "view_index_dir"),
                  couch_util:to_list(DbName), "*.geo*"]),

        % this can be slow for now, speed up later
        % lists:member is slow
        lists:foreach(fun(Sig) ->
             % sig should be the name of each Sig after path is removed
             ASig = filename:rootname(filename:basename(Sig)),
             case lists:member(ASig, ActiveSigs) of
                 false ->
                     % get the erl_spatial index and destory it
                     case hastings_index_manager:get_index(DbName, Sig) of
                         {ok, Pid} ->
                             hastings_index:delete_index(Pid);
                         _ ->
                             ok
                     end;
                 _ ->
                     ok
             end
        end, FileList),
        ok
    catch error:database_does_not_exist ->
        ok
    end.

