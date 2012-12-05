%% -*- erlang-indent-level: 4;indent-tabs-mode: nil -*-
%% Copyright 2012 Cloudant

-module(hastings_rpc).
-include_lib("couch/include/couch_db.hrl").
-include("hastings.hrl").
-import(couch_query_servers, [get_os_process/1, ret_os_process/1, proc_prompt/2]).

% public api.
-export([search/4, info/3, cleanup/1, cleanup/2]).

search(DbName, DDoc, IndexName, QueryArgs) ->
    erlang:put(io_priority, {interactive, DbName}),
    {ok, Db} = get_or_create_db(DbName, []),
    #index_query_args{
        stale = Stale
    } = QueryArgs,
    {_LastSeq, MinSeq} = calculate_seqs(Db, Stale),
    case hastings_index:design_doc_to_index(DDoc, IndexName) of
        {ok, Index} ->
            case hastings_index_manager:get_index(DbName, Index) of
                {ok, Pid} ->
                    case hastings_index:await(Pid, MinSeq) of
                        ok ->
                            Result = hastings_index:search(Pid, QueryArgs),
                            rexi:reply(Result);
                        Error ->
                            rexi:reply(Error)
                    end;
                Error ->
                    rexi:reply(Error)
            end;
        Error ->
            rexi:reply(Error)
    end.

info(DbName, DDoc, IndexName) ->
    erlang:put(io_priority, {interactive, DbName}),
    case hastings_index:design_doc_to_index(DDoc, IndexName) of
        {ok, Index} ->
            case hastings_index_manager:get_index(DbName, Index) of
                {ok, Pid} ->
                    Result = hastings_index:info(Pid),
                    rexi:reply(Result);
                Error ->
                    rexi:reply(Error)
            end;
        Error ->
            rexi:reply(Error)
    end.

cleanup(DbName) ->
    % TODO
    ok.        

cleanup(DbName, ActiveSigs) ->
    % TODO
    ok.
get_or_create_db(DbName, Options) ->
    case couch_db:open_int(DbName, Options) of
    {not_found, no_db_file} ->
        twig:log(warn, "~p creating ~s", [?MODULE, DbName]),
        couch_server:create(DbName, Options);
    Else ->
        Else
    end.

calculate_seqs(Db, Stale) ->
    LastSeq = couch_db:get_update_seq(Db),
    if
        Stale == ok orelse Stale == update_after ->
            {LastSeq, 0};
        true ->
            {LastSeq, LastSeq}
    end.
