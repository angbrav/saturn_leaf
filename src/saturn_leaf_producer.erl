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

-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).
-export([partition_heartbeat/3,
         check_ready/1,
         clean_state/1,
         set_groups/2,
         set_tree/3,
         new_label/4]).

-record(state, {vclock :: dict(),
                delay,
                manager,
                labels :: list(),
                vv_remote :: dict(),
                myid}).
                
reg_name(MyId) ->  list_to_atom(integer_to_list(MyId) ++ atom_to_list(?MODULE)).

start_link(MyId, NLeaves) ->
    gen_server:start({global, reg_name(MyId)}, ?MODULE, [MyId, NLeaves], []).

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
    
init([MyId, NLeaves]) ->
    Manager = groups_manager:init_state(integer_to_list(MyId) ++ "producer"),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    GrossPrefLists = riak_core_ring:all_preflists(Ring, 1),
    Dict = lists:foldl(fun(PrefList, Acc) ->
                        {Partition, _Node} = hd(PrefList),
                        ok = saturn_proxy_vnode:init_proxy(hd(PrefList), MyId),
                        saturn_proxy_vnode:heartbeat(PrefList),
                        dict:store(Partition, 0, Acc)
                       end, dict:new(), GrossPrefLists),
    RemoteIds = lists:delete(MyId, lists:seq(0, NLeaves-1)),
    VVRemote = lists:foldl(fun(Id, Acc) ->
                            dict:store(Id, 0, Acc)
                           end, dict:new(), RemoteIds),
    Labels = ets:new(labels_producer, [ordered_set, named_table, private]),
    erlang:send_after(10, self(), deliver),
    %{ok, Delay} = groups_manager:get_delay_leaf(Manager#state_manager.tree, MyId, Manager#state_manager.nleaves),
    Delay=0,
    {ok, #state{labels=Labels, myid=MyId, vclock=Dict, delay=Delay*1000, manager=Manager, vv_remote=VVRemote}}.

handle_cast({partition_heartbeat, Partition, Clock}, S0=#state{vclock=VClock0}) ->
    VClock1 = dict:store(Partition, Clock, VClock0),
    {noreply, S0#state{vclock=VClock1}};

handle_cast({new_label, Label, Partition, IsUpdate}, S0=#state{labels=Labels, vclock=VClock0, delay=Delay, myid=MyId}) ->
    TimeStamp = dict:fetch(MyId, Label#label.timestamp),
    case IsUpdate of
        true ->
            Now = saturn_utilities:now_microsec(),
            Time = Now + Delay;
        false ->
            Time = 0
    end,
    ets:insert(Labels, {{TimeStamp, Time, Partition, Label}, in}),
    VClock1 = dict:store(Partition, TimeStamp, VClock0),
    {noreply, S0#state{vclock=VClock1}};

handle_cast(_Info, State) ->
    {noreply, State}.

handle_call(clean_state, _From, S0=#state{labels=Labels0, vclock=VClock0, vv_remote=VVRemote0}) ->
    true = ets:delete(Labels0),
    Labels1 = ets:new(labels_producer, [ordered_set, named_table, private]),
    VClock1 = lists:foldl(fun({Partition, _}, Acc) ->
                            dict:store(Partition, 0, Acc)
                          end, dict:new(), dict:to_list(VClock0)),
    VVRemote = lists:foldl(fun(Id, Acc) ->
                            dict:store(Id, 0, Acc)
                           end, dict:new(), dict:fetch_keys(VVRemote0)),
    {reply, ok, S0#state{labels=Labels1, vclock=VClock1, vv_remote=VVRemote}};

handle_call({set_groups, RGroups}, _From, S0=#state{manager=Manager}) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    GrossPrefLists = riak_core_ring:all_preflists(Ring, 1),
    lists:foreach(fun(PrefList) ->
                    ok = saturn_proxy_vnode:set_groups(hd(PrefList), RGroups)
                  end, GrossPrefLists),
    Table = Manager#state_manager.groups,
    ok = groups_manager:set_groups(Table, RGroups),
    {reply, ok, S0};

handle_call({set_tree, Tree, Leaves}, _From, S0=#state{manager=Manager0}) ->
    Paths = groups_manager:path_from_tree_dict(Tree, Leaves),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    GrossPrefLists = riak_core_ring:all_preflists(Ring, 1),
    lists:foreach(fun(PrefList) ->
                    ok = saturn_proxy_vnode:set_tree(hd(PrefList), Paths, Tree, Leaves)
                  end, GrossPrefLists),
    Manager1 = Manager0#state_manager{paths=Paths, tree=Tree, nleaves=Leaves},
    {reply, ok, S0#state{manager=Manager1, delay=0}};

handle_call(check_ready, _From, S0) ->
    {reply, ok, S0};

handle_call(Info, From, State) ->
    lager:error("Weird message: ~p. From: ~p", [Info, From]),
    {noreply, State}.

handle_info(deliver, S0=#state{myid=MyId, labels=Labels, vclock=VClock0, manager=Manager, vv_remote=VVRemote}) ->
    StableTime1 = compute_stable(VClock0),
    {ok, VVRemote1} = deliver_labels(Labels, StableTime1, MyId, Manager, VVRemote),
    erlang:send_after(?STABILIZATION_FREQ, self(), deliver),
    {noreply, S0#state{vv_remote=VVRemote1}};

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

deliver_labels(Labels, StableTime, MyId, Manager, VVRemote) ->
    case ets:first(Labels) of
        '$end_of_table' ->
            VVRemote1 = heartbeats(VVRemote, StableTime, MyId),
            {ok, VVRemote1};
        {TimeStamp, Time, _Partition, Label}=Key when TimeStamp =< StableTime ->
            Now = saturn_utilities:now_microsec(),
            case (Time<Now) of
                true ->
                    true = ets:delete(Labels, Key),
                    {ok, VVRemote1} = handle_label(Label, Manager, MyId, VVRemote),
                    deliver_labels(Labels, StableTime, MyId, Manager, VVRemote1);
                false ->
                    VVRemote1 = heartbeats(VVRemote, StableTime, MyId),
                    {ok, VVRemote1}
            end;
        _Key ->
            VVRemote1 = heartbeats(VVRemote, StableTime, MyId),
            {ok, VVRemote1}
    end.

handle_label(Label, Manager, MyId, VVRemote) ->
    TimeStamp = dict:fetch(MyId, Label#label.timestamp),
    %TimeStamp = Label#label.timestamp,
    case Label#label.operation of
        update ->
            BKey = Label#label.bkey,
            case groups_manager:get_datanodes_ids(BKey, Manager#state_manager.groups, MyId) of
                {ok, Group} ->
                    VVRemote1 = lists:foldl(fun(Id, Acc) ->
                                                saturn_leaf_converger:label(Id, Label),
                                                dict:store(Id, TimeStamp, Acc)
                                            end, VVRemote, Group),
                    {ok, VVRemote1};
                {error, Reason2} ->
                    lager:error("No replication group for bkey: ~p (~p)", [BKey, Reason2]),
                    {ok, VVRemote}
            end;
        remote_read ->
            Payload = Label#label.payload,
            To = Payload#payload_remote.to,
            saturn_leaf_converger:label(To, Label),
            VVRemote1 = dict:store(To, TimeStamp, VVRemote),
            {ok, VVRemote1};
        remote_reply ->
            Payload = Label#label.payload,
            To = Payload#payload_reply.to,
            saturn_leaf_converger:label(To, Label),
            VVRemote1 = dict:store(To, TimeStamp, VVRemote),
            {ok, VVRemote1}
    end.

heartbeats(VVRemote0, StableTime, MyId) ->
    lists:foldl(fun(Id, Acc) ->
                    Clock = dict:fetch(Id, Acc),
                    case ((Clock + ?INTER_HEARTBEAT_FREQ*1000) < StableTime) of
                        true ->
                            saturn_leaf_converger:heartbeat(Id, StableTime, MyId),
                            dict:store(Id, StableTime, Acc);
                        false ->
                            Acc
                    end
                end, VVRemote0, dict:fetch_keys(VVRemote0)).

-ifdef(TEST).

compute_stable_clock_test() ->
    Clocks = [{p1, 1}, {p2, 2}, {p3, 4}],
    Dict = dict:from_list(Clocks),
    ?assertEqual(1, compute_stable(Dict)).

-endif.
