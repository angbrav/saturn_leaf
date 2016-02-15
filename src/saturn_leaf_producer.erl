%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015-2016 INESC-ID, Instituto Superior Tecnico,
%%                         Universidade de Lisboa, Portugal
%% Copyright (c) 2015-2016 Universite Catholique de Louvain, Belgium
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%  
%% -------------------------------------------------------------------
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
         delayed_delivery/3,
         label_delivered/2,
         new_label/3]).

-record(state, {vclock :: dict(),
                delay,
                labels :: list(),
                myid}).
                
reg_name(MyId) ->  list_to_atom(integer_to_list(MyId) ++ atom_to_list(?MODULE)).

start_link(MyId) ->
    gen_server:start({global, reg_name(MyId)}, ?MODULE, [MyId], []).

partition_heartbeat(MyId, Partition, Clock) ->
    gen_server:cast({global, reg_name(MyId)}, {partition_heartbeat, Partition, Clock}).
    
new_label(MyId, Label, Partition) ->
    gen_server:cast({global, reg_name(MyId)}, {new_label, Label, Partition}).

label_delivered(MyId, Element) ->
    gen_server:cast({global, reg_name(MyId)}, {label_delivered, Element}).

init([MyId]) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    GrossPrefLists = riak_core_ring:all_preflists(Ring, 1),
    Dict = lists:foldl(fun(PrefList, Acc) ->
                        {Partition, _Node} = hd(PrefList),
                        saturn_proxy_vnode:heartbeat(PrefList, MyId),
                        dict:store(Partition, 0, Acc)
                       end, dict:new(), GrossPrefLists),
    {ok, #state{labels=orddict:new(), myid=MyId, vclock=Dict, delay=0}}.

handle_cast({partition_heartbeat, Partition, Clock}, S0) ->
    S1 = update_vclock(Partition, Clock, S0),
    S2 = deliver_labels(S1),
    {noreply, S2};

handle_cast({new_label, Label, Partition}, S0=#state{labels=Labels0}) ->
    Now = saturn_utilities:now_milisec(),
    TimeStamp = Label#label.timestamp,
    Labels1 = orddict:append(TimeStamp, {Now, Label}, Labels0),
    S1 = update_vclock(Partition, TimeStamp, S0#state{labels=Labels1}),
    S2 = deliver_labels(S1),
    {noreply, S2};

handle_cast({label_delivered, Element}, S0=#state{labels=Labels0})->
    [{TimeStamp, ListLabels}|Rest] = Labels0,
    case lists:delete(Element, ListLabels) of
        [] ->
            S1 = deliver_labels(S0#state{labels=Rest});
        Other ->
            Labels1 = [{TimeStamp, Other}] ++ Rest,
            S1 = S0#state{labels=Labels1}
    end,
    {noreply, S1};
    
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

update_vclock(Partition, TimeStamp, S0=#state{vclock=VClock0}) ->
    VClock1 = dict:store(Partition, TimeStamp, VClock0),
    S0#state{vclock=VClock1}.

deliver_labels(S0=#state{vclock=Clocks, labels=Labels0, myid=MyId, delay=Delay}) ->
    StableTime = compute_stable_time(Clocks),
    Labels1 = filter_labels(Labels0, StableTime, MyId, Delay),
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

filter_labels([], _StableTime, _MyId, _Delay)->
    [];

filter_labels([H|Rest], StableTime, MyId, Delay) ->
    {TimeStamp, ListLabels} = H,
    case TimeStamp =< StableTime of
        true ->
            Now = saturn_utilities:now_milisec(),
            {FinalStream, Leftovers} = lists:foldl(fun({Time, Label}, {FinalStream0, Leftovers0}) ->
                                                BKey = Label#label.bkey,
                                                case (Time + Delay) > Now of
                                                    true ->
                                                        {FinalStream0, Leftovers0 ++ [{Time, Label}]};
                                                    false ->
                                                        {FinalStream0 ++ [{BKey, Label}], Leftovers0 }
                                                end
                                              end, {[], []}, ListLabels),
            propagate_stream(FinalStream, MyId),
            case Leftovers of
                [] ->
                    filter_labels(Rest, StableTime, MyId, Delay);
                _ ->
                    lists:foreach(fun({Time, _Label}=Element) ->
                                    spawn(saturn_leaf_producer, delayed_delivery, [MyId, (Time+Delay)-saturn_utilities:now_milisec(), Element])
                                  end, Leftovers),
                    [{TimeStamp, Leftovers}|Rest]
            end;
        false ->
            [H|Rest]
    end.

delayed_delivery(MyId, Delay, {Time, Label}) ->
    case Delay>0 of
        true ->
            timer:sleep(trunc(Delay));
        false ->
            noop
    end,
    BKey = Label#label.bkey,
    propagate_stream([{BKey, Label}], MyId),
    saturn_leaf_producer:label_delivered(MyId, {Time, Label}).

propagate_stream(FinalStream, MyId) ->
    case ?PROPAGATION_MODE of
        naive_erlang ->
            case groups_manager_serv:filter_stream_leaf_id(FinalStream) of
                {ok, [], _} ->
                    lager:info("Nothing to send"),
                    noop;
                {ok, _, no_indexnode} ->
                    noop;
                {ok, Stream, Id} ->
                    saturn_internal_serv:handle(Id, {new_stream, Stream, MyId})
            end;
        short_tcp ->
            case groups_manager_serv:filter_stream_leaf(FinalStream) of
                {ok, [], _} ->
                    lager:info("Nothing to send"),
                    noop;
                {ok, _, no_indexnode} ->
                    noop;
                {ok, Stream, {Host, Port}} ->
                    saturn_leaf_propagation_fsm_sup:start_fsm([Port, Host, {new_stream, Stream, MyId}])
            end
    end.

-ifdef(TEST).

compute_stable_clock_test() ->
    Clocks = [{p1, 1}, {p2, 2}, {p3, 4}],
    Dict = dict:from_list(Clocks),
    ?assertEqual(1, compute_stable_time(Dict)).

-endif.
