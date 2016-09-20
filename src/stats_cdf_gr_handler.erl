-module(stats_cdf_gr_handler).

-include("saturn_leaf.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([init/1,
         add_remote/3,
         add_update/3,
         add_gst/2,
         compute_raw/3,
         merge_raw/2,
         compute_cdf_from_orddict/1,
         clean/2]).

init(Name) ->
    NUpdates = list_to_atom(atom_to_list(Name) ++ "_updates"),
    NRemotes = list_to_atom(atom_to_list(Name) ++ "_remotes"),
    Updates = ets:new(NUpdates, [set, named_table, private]),
    Remotes = ets:new(NRemotes, [set, named_table, private]),
    {0, Updates, 0, Remotes, [], {0,0}}.

clean(Data, Name) ->
    {_IdUp, Updates, _IdRem, Remotes} = Data,
    true = ets:delete(Updates),
    true = ets:delete(Remotes),
    NUpdates = list_to_atom(atom_to_list(Name) ++ "_updates"),
    NRemotes = list_to_atom(atom_to_list(Name) ++ "_remotes"),
    Updates = ets:new(NUpdates, [set, named_table, private]),
    Remotes = ets:new(NRemotes, [set, named_table, private]),
    {0, Updates, 0, Remotes, [], {0,0}}.

add_update(Data, Sender, TimeStamp) ->
    %Dif = saturn_utilities:now_microsec() - TimeStamp,
    {IdUp, Updates, IdRem, Remotes, Pending, Stable} = Data,
    case Sender of
        ?SENDER_STALENESS ->
            {GST, When} = Stable,
            case GST >= TimeStamp of
                true ->
                    ets:insert(Updates, {IdUp, {Sender, When - TimeStamp}}),
                    {IdUp+1, Updates, IdRem, Remotes, Pending, Stable};
                false ->
                    {IdUp, Updates, IdRem, Remotes, [{TimeStamp, Sender}|Pending], Stable}
            end;
        _ ->
            {IdUp, Updates, IdRem, Remotes, Pending, Stable}
    end.

add_remote(Data, Sender, TimeStamp) ->
    %Dif = saturn_utilities:now_microsec() - TimeStamp,
    {IdUp, Updates, IdRem, Remotes, Pending, Stable} = Data,
    true = ets:insert(Remotes, {IdRem, {Sender, TimeStamp}}),
    {IdUp, Updates, IdRem+1, Remotes, Pending, Stable}.

add_gst(Data, GST) ->
    {IdUp, Updates, IdRem, Remotes, Pending, Stable} = Data,
    {MaxGST,_} = Stable,
    case GST > MaxGST of
        true ->
            Time = saturn_utilities:now_microsec(),
            case process_pending(Pending, GST, Time, Updates, IdUp, []) of
                {ok, _, IdUp} ->
                    {IdUp, Updates, IdRem, Remotes, Pending, {GST, Time}};
                {ok, Pending1, IdUp1} ->
                    {IdUp1, Updates, IdRem, Remotes, Pending1, {GST, Time}}
            end;
        false ->
            {IdUp, Updates, IdRem, Remotes, Pending, Stable}
    end.

process_pending([], _GST, _When, _Updates, IdUp, _NewPending) ->
    {ok, whole, IdUp};

process_pending([Next|Rest]=List, GST, When, Updates, IdUp, NewPending) ->
    {Time, _Sender} = Next,
    case Time > GST of
        true ->
            process_pending(Rest, GST, When, Updates, IdUp, [Next|NewPending]);
        false ->
            IdUp1=lists:foldl(fun({ElemTime, ElemSender}, Acc) ->
                                ets:insert(Updates, {Acc, {ElemSender, When - ElemTime}}),
                                Acc+1
                              end, IdUp, List),
            {ok, IdUp1, lists:reverse(NewPending)}
    end.

compute_raw(Data, From, Type) ->
    case Type of
        updates ->
            {IdUp, Updates, _IdRem, _Remotes, _Pending, _Stable} = Data,
            get_ordered(From, Updates, IdUp);
        remotes ->
            {_IdUp, _Updates, IdRem, Remotes, _Pending, _Stable} = Data,
            get_ordered(From, Remotes, IdRem)
    end.

merge_raw(FinalList, NewList) ->
    lists:foldl(fun({Time, _List}, Acc) ->
                    orddict:append(Time, v, Acc)
                end, FinalList, NewList).

compute_cdf_from_orddict(List) ->
    ListSteps = get_liststeps(List),
    generate_percentiles(List, 0, ListSteps, []).

get_liststeps(List) ->
    Total = compute_size(List, 0),
    Step = Total / ?PERCENTILES,
    ListSteps = lists:foldl(fun(Number, Acc) ->
                                Acc ++ [trunc(Step*Number)]
                            end, [], lists:seq(1, ?PERCENTILES-1)),
    ListSteps ++ [Total].

compute_size([], Counter) ->
    Counter;

compute_size([Next|Rest], Counter)->
    {_Time, List} = Next,
    compute_size(Rest, Counter + length(List)).

get_ordered(From, Table, Id) ->
    lists:foldl(fun(Key, List0) ->
                    [{Key, {Sender, Time}}] = ets:lookup(Table, Key),
                    case Sender of
                        From ->
                            orddict:append(Time, v, List0);
                        _ ->
                            List0
                    end
                end, [], lists:seq(0, Id-1)).

generate_percentiles([], _Counter, [], Times) ->
    Times;

generate_percentiles([Next|Rest], Counter, [NextStep|RestSteps]=Steps, Times) ->
    {Time, List} = Next,
    case (length(List) + Counter) >= NextStep of
        true ->
            generate_percentiles(Rest, Counter+length(List), RestSteps, Times ++ [Time]);
        false ->
            generate_percentiles(Rest, Counter+length(List), Steps, Times)
    end.
    
-ifdef(TEST).

get_ordered_test() ->
    Table = ets:new(test_gr_handler, [set, named_table, private]),
    true = ets:insert(Table, {0, {sender1, 60}}),
    true = ets:insert(Table, {1, {sender1, 30}}),
    true = ets:insert(Table, {2, {sender2, 70}}),
    true = ets:insert(Table, {3, {sender1, 90}}),
    true = ets:insert(Table, {4, {sender1, 10}}),
    ?assertEqual([{10, [v]}, {30, [v]}, {60, [v]}, {90, [v]}], get_ordered(sender1, Table, 5)). 

generate_liststeps_test() ->
    List = [{1, [v]}, {2, [v]}, {3, [v]},
            {4, [v]}, {5, [v]}, {6, [v]}, 
            {7, [v]}, {8, [v]}, {9, [v]},
            {10, [v]}, {11, [v]}, {12, [v]}, 
            {13, [v]}, {14, [v]}, {15, [v]}, 
            {16, [v]}, {17, [v]}, {18, [v]}, 
            {19, [v]}, {20, [v]}, {21, [v]}],
    ?assertEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 21], get_liststeps(List)).

generate_percentiles_test() ->
    List = [{10, [v]}, {20, [v]}, {30, [v]},
            {40, [v]}, {50, [v]}, {60, [v]}, 
            {70, [v]}, {80, [v]}, {90, [v]},
            {100, [v]}, {110, [v]}, {120, [v]}, 
            {130, [v]}, {140, [v]}, {150, [v]}, 
            {160, [v]}, {170, [v]}, {180, [v]}, 
            {190, [v]}, {200, [v]}, {210, [v]}],
    ListSteps = get_liststeps(List),
    ?assertEqual([10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 210], generate_percentiles(List, 0, ListSteps, [])).

-endif.
