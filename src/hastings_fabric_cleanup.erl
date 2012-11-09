%% -*- erlang-indent-level: 4;indent-tabs-mode: nil -*-
%% Copyright 2012 Cloudant

-module(hastings_fabric_cleanup).

-include("hastings.hrl").
-include_lib("mem3/include/mem3.hrl").
-include_lib("couch/include/couch_db.hrl").

-export([go/1]).

go(DbName) ->
    {ok, DesignDocs} = fabric:design_docs(DbName),
    _ActiveSigs = lists:usort(lists:flatmap(fun active_sigs/1,
        [couch_doc:from_json_obj(DD) || DD <- DesignDocs])),
%    clouseau_rpc:cleanup(DbName, ActiveSigs),
    ok.

active_sigs(#doc{body={Fields}}=Doc) ->
    {RawIndexes} = couch_util:get_value(<<"indexes">>, Fields, {[]}),
    {IndexNames, _} = lists:unzip(RawIndexes),
    [begin
         {ok, Index} = hastings_index:design_doc_to_index(Doc, IndexName),
         Index#index.sig
     end || IndexName <- IndexNames].
