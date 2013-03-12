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
    cleanup(DbName, []).        

cleanup(DbName, ActiveSigs) ->
    % get all geo indexes for this database
    % leave active indexes
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
