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
         check_ready/1,
         clean_state/1,
         set_tree/3,
         set_groups/2,
         dump_stats/1,
         new_label/4]).

-record(state, {vclock :: dict(),
                delay,
                stable_time,
                manager,
                pending,
                labels :: list(),
                propagation_stats,
                myid}).
                
reg_name(MyId) ->  list_to_atom(integer_to_list(MyId) ++ atom_to_list(?MODULE)).

start_link(MyId) ->
    gen_server:start({global, reg_name(MyId)}, ?MODULE, [MyId], []).

partition_heartbeat(MyId, Partition, Clock) ->
    gen_server:cast({global, reg_name(MyId)}, {partition_heartbeat, Partition, Clock}).
    
new_label(MyId, Label, Partition, IsUpdate) ->
    gen_server:cast({global, reg_name(MyId)}, {new_label, Label, Partition, IsUpdate}).

check_ready(MyId) ->
    gen_server:call({global, reg_name(MyId)}, check_ready, infinity).

clean_state(MyId) ->
    gen_server:call({global, reg_name(MyId)}, clean_state, infinity).

set_tree(MyId, TreeDict, NLeaves) ->
    gen_server:call({global, reg_name(MyId)}, {set_tree, TreeDict, NLeaves}, infinity).

set_groups(MyId, Groups) ->
    gen_server:call({global, reg_name(MyId)}, {set_groups, Groups}, infinity).
    
dump_stats(MyId) ->
    gen_server:call({global, reg_name(MyId)}, dump_stats, infinity).

init([MyId]) ->
    Manager = groups_manager:init_state(integer_to_list(MyId) ++ "producer"),
    PropagationStats = ets:new(propagation, [ordered_set, named_table, private]),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    GrossPrefLists = riak_core_ring:all_preflists(Ring, 1),
    Dict = lists:foldl(fun(PrefList, Acc) ->
                        {Partition, _Node} = hd(PrefList),
                        ok = saturn_proxy_vnode:init_proxy(hd(PrefList), MyId),
                        saturn_proxy_vnode:heartbeat(PrefList),
                        dict:store(Partition, 0, Acc)
                       end, dict:new(), GrossPrefLists),
    Labels = ets:new(labels_producer, [ordered_set, named_table, private]),
    erlang:send_after(10, self(), deliver),
    %{ok, Delay} = groups_manager_serv:get_delay_leaf(),
    Delay=0,
    {ok, #state{labels=Labels, myid=MyId, vclock=Dict, delay=Delay*1000, stable_time=0, pending=false, manager=Manager, propagation_stats=PropagationStats}}.


handle_cast({partition_heartbeat, Partition, Clock}, S0=#state{vclock=VClock0, pending=_Pending0, stable_time=_StableTime0, labels=_Labels, myid=_MyId}) ->
    VClock1 = dict:store(Partition, Clock, VClock0),
    %StableTime1 = compute_stable(VClock1),
    %case (Pending0==false) and (StableTime1 > StableTime0) of
        %true ->
            %Pending1 = deliver_labels(Labels, StableTime1, MyId, []),
            %{noreply, S0#state{vclock=VClock1, stable_time=StableTime1, pending=Pending1}};
        %false ->
            %{noreply, S0#state{vclock=VClock1, stable_time=StableTime1}}
    %end;
    {noreply, S0#state{vclock=VClock1}};

handle_cast({new_label, Label, Partition, _IsUpdate}, S0=#state{labels=Labels, vclock=VClock0, stable_time=_StableTime0, myid=MyId, pending=_Pending0, delay=_Delay, propagation_stats=PropagationStats}) ->
    Now = saturn_utilities:now_microsec(),
    true = ets:insert(PropagationStats, {Label, {MyId, Now}}),
    TimeStamp = Label#label.timestamp,
    Time=0,
    %case IsUpdate of
    %    true ->
    %        Now = saturn_utilities:now_microsec(),
    %        Time = Now + Delay;
    %    false ->
    %        Time = 0
    %end,
    ets:insert(Labels, {{TimeStamp, Time, Partition, Label}, in}),
    VClock1 = dict:store(Partition, TimeStamp, VClock0),
    %StableTime1 = compute_stable(VClock1),
    %case (Pending0==false) and (StableTime1 > StableTime0) of
    %    true ->
    %        Pending1 = deliver_labels(Labels, StableTime1, MyId, []),
    %        {noreply, S0#state{vclock=VClock1, stable_time=StableTime1, pending=Pending1}};
    %    false ->
    %        {noreply, S0#state{vclock=VClock1, stable_time=StableTime1}}
    %end;
    {noreply, S0#state{vclock=VClock1}};

handle_cast(_Info, State) ->
    {noreply, State}.

handle_call(dump_stats, _From, S0=#state{propagation_stats=PropagationStats, myid=MyId}) ->
    ok = ets:tab2file(PropagationStats, list_to_atom(integer_to_list(MyId) ++ atom_to_list('-propagation.txt'))),
    {reply, ok, S0};

handle_call(clean_state, _From, S0=#state{labels=Labels0, vclock=VClock0, propagation_stats=PropagationStats0}) ->
    true = ets:delete(Labels0),
    Labels1 = ets:new(labels_producer, [ordered_set, named_table, private]),
    true = ets:delete(PropagationStats0),
    PropagationStats1 = ets:new(propagation, [ordered_set, named_table, private]),
    VClock1 = lists:foldl(fun({Partition, _}, Acc) ->
                            dict:store(Partition, 0, Acc)
                          end, dict:new(), dict:to_list(VClock0)),
    {reply, ok, S0#state{labels=Labels1, vclock=VClock1, stable_time=0, pending=false, propagation_stats=PropagationStats1}};

handle_call({set_tree, Tree, Leaves}, _From, S0=#state{manager=Manager0}) ->
    Paths = groups_manager:path_from_tree_dict(Tree, Leaves),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    GrossPrefLists = riak_core_ring:all_preflists(Ring, 1),
    lists:foreach(fun(PrefList) ->
                    ok = saturn_proxy_vnode:set_tree(hd(PrefList), Paths, Tree, Leaves)
                  end, GrossPrefLists),
    Manager1 = Manager0#state_manager{paths=Paths, tree=Tree, nleaves=Leaves},
    {reply, ok, S0#state{manager=Manager1}};

handle_call({set_groups, RGroups}, _From, S0=#state{manager=Manager}) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    GrossPrefLists = riak_core_ring:all_preflists(Ring, 1),
    lists:foreach(fun(PrefList) ->
                    ok = saturn_proxy_vnode:set_groups(hd(PrefList), RGroups)
                  end, GrossPrefLists),
    Table = Manager#state_manager.groups,
    ok = groups_manager:set_groups(Table, RGroups),
    {reply, ok, S0};

handle_call(check_ready, _From, S0) ->
    {reply, ok, S0};

handle_call(Info, From, State) ->
    lager:error("Weird message: ~p. From: ~p", [Info, From]),
    {noreply, State}.

handle_info(find_deliverables, S0=#state{stable_time=StableTime0, myid=MyId, labels=Labels}) ->
    Pending1 = deliver_labels(Labels, StableTime0, MyId, []),
    {noreply, S0#state{pending=Pending1}};

handle_info(deliver, S0=#state{myid=MyId, labels=Labels, vclock=VClock0, manager=Manager}) ->
    StableTime1 = compute_stable(VClock0),
    ok = deliver_labels_new(Labels, StableTime1, MyId, [], Manager),
    erlang:send_after(1, self(), deliver),
    {noreply, S0};

handle_info(Info, State) ->
    lager:info("Weird message: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

compute_stable(VClock) ->
    lists:foldl(fun({_Partition, Clock}, Min) ->
                    min(Clock, Min)
                 end, infinity, dict:to_list(VClock)).

deliver_labels(Labels, StableTime, MyId, Deliverables0) ->
    case ets:first(Labels) of
        '$end_of_table' ->
            propagate_stream(lists:reverse(Deliverables0), MyId, manager),
            false;
        {TimeStamp, Time, _Partition, Label}=Key when TimeStamp =< StableTime ->
            Now = saturn_utilities:now_microsec(),
            case Time > Now of
                true ->
                    propagate_stream(lists:reverse(Deliverables0), MyId, manager),
                    NextDelivery = trunc((Time - Now)/1000),
                    erlang:send_after(NextDelivery, self(), find_deliverables),                   
                    true;
                false ->
                    true = ets:delete(Labels, Key),
                    BKey = Label#label.bkey,
                    deliver_labels(Labels, StableTime, MyId, [{BKey, Label}|Deliverables0]),
                    false
            end;
        _Key ->
            propagate_stream(lists:reverse(Deliverables0), MyId, manager),
            false
    end.

deliver_labels_new(Labels, StableTime, MyId, Deliverables0, Manager) ->
    case ets:first(Labels) of
        '$end_of_table' ->
            propagate_stream(lists:reverse(Deliverables0), MyId, Manager),
            ok;
        {TimeStamp, _Time, _Partition, Label}=Key when TimeStamp =< StableTime ->
            true = ets:delete(Labels, Key),
            BKey = Label#label.bkey,
            deliver_labels_new(Labels, StableTime, MyId, [{BKey, Label}|Deliverables0], Manager),
            ok;
        _Key ->
            propagate_stream(lists:reverse(Deliverables0), MyId, Manager),
            ok
    end.

propagate_stream(FinalStream, MyId, Manager) ->
    Tree = Manager#state_manager.tree,
    NLeaves = Manager#state_manager.nleaves,
    Groups = Manager#state_manager.groups,
    Paths = Manager#state_manager.paths,
    case groups_manager:filter_stream_leaf_id(FinalStream, Tree, NLeaves, MyId, Groups, Paths) of
        {ok, [], _} ->
            %lager:info("Nothing to send"),
            noop;
        {ok, _, no_indexnode} ->
            noop;
        {ok, Stream, Id} ->
            saturn_internal_serv:handle(Id, {new_stream, Stream, MyId})
    end.

-ifdef(TEST).

compute_stable_clock_test() ->
    Clocks = [{p1, 1}, {p2, 2}, {p3, 4}],
    Dict = dict:from_list(Clocks),
    ?assertEqual(1, compute_stable(Dict)).

-endif.
