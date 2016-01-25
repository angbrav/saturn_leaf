-module(saturn_leaf_producer).
-behaviour(gen_server).

-include("saturn_leaf.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).
-export([partition_heartbeat/3,
         new_label/3]).

-record(state, {vclock :: dict(),
                labels :: list(),
                myid}).
                
reg_name(MyId) ->  list_to_atom(integer_to_list(MyId) ++ atom_to_list(?MODULE)).

start_link(MyId) ->
    gen_server:start({global, reg_name(MyId)}, ?MODULE, [MyId], []).

partition_heartbeat(MyId, Partition, Clock) ->
    gen_server:cast({global, reg_name(MyId)}, {partition_heartbeat, Partition, Clock}).
    
new_label(MyId, Label, Partition) ->
    gen_server:cast({global, reg_name(MyId)}, {new_label, Label, Partition}).

init([MyId]) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    GrossPrefLists = riak_core_ring:all_preflists(Ring, 1),
    Dict = lists:foldl(fun(PrefList, Acc) ->
                        {Partition, _Node} = hd(PrefList),
                        saturn_proxy_vnode:heartbeat(PrefList, MyId),
                        dict:store(Partition, 0, Acc)
                       end, dict:new(), GrossPrefLists),
    {ok, #state{labels=orddict:new(), myid=MyId, vclock=Dict, seqs=dict:new()}}.

handle_cast({partition_heartbeat, Partition, Clock}, S0}) ->
    S1 = update_vclock(Seq, PendingTS0, Partition, Clock, S0),
    S2 = deliver_labels(S1);
    {noreply, S2};

handle_cast({new_label, Label, Partition}, S0=#state{labels=Labels0}) ->
    {_Key, TimeStamp, _Node} = Label,
    case dict:find(Partition, Seqs0) of
        {ok, Value} ->
            {StableSeq, PendingTS0} = Value;
        error ->
            {StableSeq, PendingTS0} = {0, []}
    end,
    Labels1 = orddict:append(TimeStamp, Label, Labels0),
    case ((StableSeq + 1) == Seq) of
        true ->
            S1 = update_vclock(Seq, PendingTS0, Partition, TimeStamp, S0#state{labels=Labels1}),
            S2 = deliver_labels(S1);
        false ->
            PendingTS1 = orddict:store(Seq, TimeStamp, PendingTS0),
            Seqs1 = dict:store(Partition, {StableSeq, PendingTS1}, Seqs0),
            S2 = S0#state{labels=Labels1, seqs=Seqs1}
    end,
    {noreply, S2};

handle_cast(_Info, State) ->
    {noreply, State}.

handle_call(Info, From, State) ->
    lager:error("Weird message: ~p. From: ~p", [Info, From]),
    {noreply, State}.

handle_info(Info, State) ->
    lager:info("Weird message: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

update_vclock(StableSeq, [], Partition, TimeStamp, S0=#state{seqs=Seqs0, vclock=VClock0}) ->
    VClock1 = dict:store(Partition, TimeStamp, VClock0),
    Seqs1 = dict:store(Partition, {StableSeq, []}, Seqs0), 
    S0#state{vclock=VClock1, seqs=Seqs1};

update_vclock(StableSeq0, [Next|Rest]=PendingTS, Partition, TimeStamp, S0=#state{seqs=Seqs0, vclock=VClock0}) ->
    {NextSeq, NextTimeStamp} = Next,
    case ((StableSeq0 + 1) == NextSeq) of
        true ->
            update_vclock(NextSeq, Rest, Partition, NextTimeStamp, S0);
        false ->
            VClock1 = dict:store(Partition, TimeStamp, VClock0),
            Seqs1 = dict:store(Partition, {StableSeq0, PendingTS}, Seqs0), 
            S0#state{vclock=VClock1, seqs=Seqs1}
    end.

deliver_labels(S0=#state{vclock=Clocks, labels=Labels0, myid=MyId}) ->
    StableTime = compute_stable_time(Clocks),
    Labels1 = filter_labels(Labels0, StableTime, MyId),
    S0#state{labels=Labels1}.

compute_stable_time(Clocks) ->
    ListClocks = dict:to_list(Clocks),
    [First|Rest] = ListClocks,
    {_FirstPartition, FirstClock} = First,
    lists:foldl(fun({_Partition, Clock}, Min) ->
                    case Clock<Min of
                        true ->
                            Clock;
                        false ->
                            Min
                    end
                end, FirstClock, Rest). 

filter_labels([], _StableTime, _MyId)->
    [];

filter_labels([H|Rest], StableTime, MyId) ->
    {TimeStamp, ListLabels} = H,
    case TimeStamp =< StableTime of
        true ->
            case groups_manager_serv:filter_stream_leaf(ListLabels) of
                {ok, [], _} ->
                    noop;
                {ok, _, no_indexnode} ->
                    noop;
                {ok, Stream, {Host, Port}} ->
                    saturn_leaf_propagation_fsm_sup:start_fsm([Port, Host, {new_stream, Stream, MyId}])
            end,
            filter_labels(Rest, StableTime, MyId);
        false ->
            [H|Rest]
    end.

-ifdef(TEST).

compute_stable_clock_test() ->
    Clocks = [{p1, 1}, {p2, 2}, {p3, 4}],
    Dict = dict:from_list(Clocks),
    ?assertEqual(1, compute_stable_time(Dict)).

update_vclock_test() ->
    StableSeq0 = 3,
    PendingTS = [{4, 7}, {5, 11}, {8, 18}, {9, 31}],
    Partition = part1,
    TimeStamp = 6,
    S = update_vclock(StableSeq0, PendingTS, Partition, TimeStamp, #state{seqs=dict:new(), vclock=dict:new()}),
    Seqs1 = S#state.seqs,
    VClock1 = S#state.vclock,
    
    {StableSeq1, PendingTS1} = dict:fetch(Partition, Seqs1),
    TS1 = dict:fetch(Partition, VClock1),
    
    ?assertEqual(5, StableSeq1),
    ?assertEqual([{8, 18}, {9, 31}], PendingTS1),
    ?assertEqual(11, TS1),

    S2 = update_vclock(6, [{8, 18}, {9, 31}], Partition, 15, S),
    Seqs2 = S2#state.seqs,
    VClock2 = S2#state.vclock,
    
    {StableSeq2, PendingTS2} = dict:fetch(Partition, Seqs2),
    TS2 = dict:fetch(Partition, VClock2),
    
    ?assertEqual(6, StableSeq2),
    ?assertEqual([{8, 18}, {9, 31}], PendingTS2),
    ?assertEqual(15, TS2).

-endif.
