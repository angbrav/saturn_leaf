-module(stats_cdf_cure_handler).

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
    {0, Updates, 0, Remotes, [], {dict:new(),0}}.

clean(Data, Name) ->
    {_IdUp, Updates, _IdRem, Remotes, _, _} = Data,
    true = ets:delete(Updates),
    true = ets:delete(Remotes),
    NUpdates = list_to_atom(atom_to_list(Name) ++ "_updates"),
    NRemotes = list_to_atom(atom_to_list(Name) ++ "_remotes"),
    Updates = ets:new(NUpdates, [set, named_table, private]),
    Remotes = ets:new(NRemotes, [set, named_table, private]),
    {0, Updates, 0, Remotes, [], {dict:new(),0}}.

add_update(Data, Sender, TimeStamp) ->
    Clock = dict:fetch(Sender, TimeStamp),
    Dif = saturn_utilities:now_microsec() - Clock,
    {IdUp, Updates, IdRem, Remotes, Pending, Stable} = Data,
    case Sender of
        ?SENDER_STALENESS ->
            ets:insert(Updates, {IdUp, {Sender, Dif}}),
            case IdUp rem 1000 of
                0 ->
                    lager:info("Remote update dif: ~p" ,[Dif]);
                _ ->
                    noop
            end,
            {IdUp+1, Updates, IdRem, Remotes, Pending, Stable};
            %lager:info("Update timestamp: ~p", [dict:to_list(TimeStamp)]),
            %{IdUp, Updates, IdRem, Remotes, [{TimeStamp, dict:fetch(Sender, TimeStamp), Sender}|Pending], Stable};
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
    case is_dominant(dict:to_list(GST), MaxGST, false)  of
        true ->
            Time = saturn_utilities:now_microsec(),
            ClockGST = dict:fetch(?SENDER_STALENESS, GST),
            case process_pending(Pending, ClockGST, GST, Time, Updates, IdUp, []) of
                {ok, _, IdUp} ->
                    {IdUp, Updates, IdRem, Remotes, Pending, {GST, Time}};
                {ok, Pending1, IdUp1} ->
                    {IdUp1, Updates, IdRem, Remotes, Pending1, {GST, Time}}
            end;
        false ->
            {IdUp, Updates, IdRem, Remotes, Pending, Stable}
    end.

process_pending([], _ClockGST, _GST, _When, _Updates, IdUp, _NewPending) ->
    {ok, whole, IdUp};

process_pending([Next|Rest]=List, ClockGST, GST, When, Updates, IdUp, NewPending) ->
    {_TimeStamp, ClockUp, _Sender} = Next,
    case ClockUp > ClockGST of
        true ->
            process_pending(Rest, ClockGST, GST, When, Updates, IdUp, [Next|NewPending]);
        false ->
            {NewPending1, IdUp1}=lists:foldl(fun({ElemTS, ElemTime, ElemSender}=Elem, {IdUp0, Pending}) ->
                                                case is_stable(dict:to_list(GST), ElemTS)  of
                                                    true ->
                                                        ets:insert(Updates, {IdUp0, {ElemSender, When - ElemTime}}),
                                                        {IdUp0+1, Pending};
                                                    false ->
                                                        {IdUp0, [Elem|Pending]}
                                                end
                                             end, {IdUp, NewPending}, List),
            {ok, lists:reverse(NewPending1), IdUp1}
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

is_stable([], _TimeStamp) ->
    true;

is_stable([{DC, Clock}|T], TimeStamp) ->
    case dict:find(DC, TimeStamp) of
        {ok, Clock2} when (Clock>=Clock2) ->
            is_stable(T, TimeStamp);
        error ->
            is_stable(T, TimeStamp);
        _ ->
            false
    end.

is_dominant([], _TimeStamp, Greater) ->
    Greater;

is_dominant([{DC, Clock}|T], TimeStamp, Greater) ->
    case dict:find(DC, TimeStamp) of
        {ok, Clock2} when (Clock>Clock2) ->
            is_dominant(T, TimeStamp, true);
        {ok, Clock2} when (Clock==Clock2) ->
            is_dominant(T, TimeStamp, Greater);
        error ->
            is_dominant(T, TimeStamp, Greater);
        _ ->
            false
    end.
    
-ifdef(TEST).

get_ordered_test() ->
    Table = ets:new(test_cure_handler, [set, named_table, private]),
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

is_dominant_test() ->
    D1 = dict:store(dc1, 3, dict:new()),
    D2 = dict:store(dc2, 4, D1),
    D3 = dict:store(dc3, 5, D2),

    D4 = dict:store(dc1, 3, dict:new()),
    D5 = dict:store(dc2, 4, D4),
    D6 = dict:store(dc3, 5, D5),

    D7 = dict:store(dc1, 3, dict:new()),
    D8 = dict:store(dc2, 4, D7),
    D9 = dict:store(dc3, 6, D8),

    ?assertEqual(true, is_dominant(dict:to_list(D9), D6, false)), 
    ?assertEqual(false, is_dominant(dict:to_list(D3), D6, false)).

-endif.
