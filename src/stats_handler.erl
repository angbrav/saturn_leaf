-module(stats_handler).

-include("saturn_leaf.hrl").

-export([init/0,
         clean/1,
         add_remote/2,
         add_update/2,
         compute_averages/1]).

init() ->
    ets:new(staleness, [set, named_table, private]).

clean(Table) ->
    true = ets:delete(Table),
    ets:new(staleness, [set, named_table, private]).

add_remote(Table, Label) ->
    Sender = Label#label.sender,
    Dif = saturn_utilities:now_microsec() - Label#label.timestamp,
    case ets:lookup(Table, Sender) of
        [] ->
            ets:insert(Table, {Sender, {0, 0, Dif, 1}});
        [{Sender, {SUpdates, TUpdates, SRemotes, TRemotes}}] ->
            ets:insert(Table, {Sender, {SUpdates, TUpdates, SRemotes+Dif, TRemotes+1}})
    end.

add_update(Table, Label) ->
    Sender = Label#label.sender,
    Dif = saturn_utilities:now_microsec() - Label#label.timestamp,
    %lager:info("Loggin label ~p, dif: ~p", [Label, Dif]),
    case ets:lookup(Table, Sender) of
        [] ->
            ets:insert(Table, {Sender, {Dif, 1, 0, 0}});
        [{Sender, {SUpdates, TUpdates, SRemotes, TRemotes}}] ->
            ets:insert(Table, {Sender, {SUpdates+Dif, TUpdates+1, SRemotes, TRemotes}})
    end.
compute_averages(Table) ->
    compute_averages_internal(ets:first(Table), Table, dict:new()).

compute_averages_internal('$end_of_table', _Table, Dict) ->
    Dict;
    
compute_averages_internal(Sender, Table, Dict) ->
    case ets:lookup(Table, Sender) of
        [{Sender, {_, 0, SRemotes, TRemotes}}] ->
            Dict1 = dict:store(Sender, {{remote_reads, trunc(SRemotes/(TRemotes*1000))}}, Dict),
            compute_averages_internal(ets:next(Table, Sender), Table, Dict1);
        [{Sender, {SUpdates, TUpdates, _, 0}}] ->
            Dict1 = dict:store(Sender, {{updates, trunc(SUpdates/(TUpdates*1000))}}, Dict),
            compute_averages_internal(ets:next(Table, Sender), Table, Dict1);
        [{Sender, {SUpdates, TUpdates, SRemotes, TRemotes}}] ->
            Dict1 = dict:store(Sender, {{updates, trunc(SUpdates/(TUpdates*1000))}, {remote_reads, trunc(SRemotes/(TRemotes*1000))}}, Dict),
            compute_averages_internal(ets:next(Table, Sender), Table, Dict1);
        [Else] ->
            lager:error("Something is wrong with the stat format: ~p", [Else]),
            compute_averages_internal(ets:next(Table, Sender), Table, Dict)
    end.
