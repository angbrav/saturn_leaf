-module(stats_handler).

-include("saturn_leaf.hrl").

-export([add_remote/2,
         add_update/2,
         compute_averages/2]).

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
    case ets:lookup(Table, Sender) of
        [] ->
            ets:insert(Table, {Sender, {Dif, 1, 0, 0}});
        [{Sender, {SUpdates, TUpdates, SRemotes, TRemotes}}] ->
            ets:insert(Table, {Sender, {SUpdates+Dif, TUpdates+1, SRemotes, TRemotes}})
    end.

compute_averages(Table, NLeaves) ->
    Result = lists:foldl(fun(Sender, Acc) ->
                            case ets:lookup(Table, Sender) of
                                [] ->
                                    Acc;
                                [{Sender, {SUpdates, TUpdates, SRemotes, TRemotes}}] ->
                                    dict:store(Sender, {{updates, trunc(SUpdates/TUpdates*1000)}, {remote_reads, trunc(SRemotes/TRemotes*1000)}}, Acc)
                            end
                         end, dict:new(), lists:seq(0, NLeaves-1)),
    Result.
