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
-module(one_leaf_test).

-export([confirm/0]).

-export([read_updates_different_nodes/1,
         converger_no_interleaving/1,
         converger_interleaving/1]).

-include("saturn_leaf.hrl").

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    ok = saturn_test_utilities:clean_datastore_data([1]),

    NumVNodes = rt_config:get(num_vnodes, 8),
    rt:update_app_config(all,[
        {riak_core, [{ring_creation_size, NumVNodes}]}
    ]),
    rt:update_app_config(all,[
        {saturn_leaf, [{riak_port, 8001}]}
    ]),
    rt:update_app_config(all,[
        {saturn_leaf, [{groups, "data/manager/groups.saturn"}]}
    ]),
    rt:update_app_config(all,[
        {saturn_leaf, [{tree, "data/manager/tree.saturn"}]}
    ]),
    N = 2,
    [Cluster1, Cluster2, Cluster3] = rt:build_clusters([N, 1, 2]),

    lager:info("Waiting for ring to converge."),
    rt:wait_until_ring_converged(Cluster1),
    rt:wait_until_ring_converged(Cluster2),
    rt:wait_until_ring_converged(Cluster3),

    [Node1, _Node2] = Cluster1,
    Leaf2 = hd(Cluster2),
    Internal1 = hd(Cluster3),
    

    pong = rpc:call(Node1, net_adm, ping, [Leaf2]),
    pong = rpc:call(Node1, net_adm, ping, [Internal1]),
    pong = rpc:call(Leaf2, net_adm, ping, [Internal1]),

    rt:wait_for_service(Leaf2, saturn_proxy),
    rt:wait_for_service(Node1, saturn_proxy),

    %% Starting servers in one node (Node1)
    {ok, _HostPort}=rpc:call(Node1, saturn_leaf_sup, start_leaf, [4040, 0]),
    {ok, _HostPort}=rpc:call(Leaf2, saturn_leaf_sup, start_leaf, [4040, 1]),

    ok=rpc:call(Node1, saturn_leaf_producer, check_ready, [0]),
    ok=rpc:call(Leaf2, saturn_leaf_producer, check_ready, [1]),

    ok=rpc:call(Node1, saturn_leaf_receiver, assign_convergers, [0, 2]),
    ok=rpc:call(Leaf2, saturn_leaf_receiver, assign_convergers, [1, 2]),

    {ok, _HostPort}=rpc:call(Internal1, saturn_leaf_sup, start_internal, [4040, 2]),

    Tree0 = dict:store(0, [-1, 300, 50], dict:new()),
    Tree1 = dict:store(1, [300, -1, 70], Tree0),
    Tree2 = dict:store(2, [50, 70, -1], Tree1),

    Groups0 = dict:store(1, [0, 1], dict:new()),
    Groups1 = dict:store(2, [0, 1], Groups0),
    Groups2 = dict:store(3, [0, 1], Groups1),

    ok = rpc:call(Node1, saturn_leaf_producer, set_tree, [0, Tree2, 2]),
    ok = rpc:call(Node1, saturn_leaf_producer, set_groups, [0, Groups2]),

    ok = rpc:call(Leaf2, saturn_leaf_producer, set_tree, [1, Tree2, 2]),
    ok = rpc:call(Leaf2, saturn_leaf_producer, set_groups, [1, Groups2]),

    ok = rpc:call(Internal1, saturn_internal_serv, set_tree, [2, Tree2, 2]),
    ok = rpc:call(Internal1, saturn_internal_serv, set_groups, [2, Groups2]),
    
    read_updates_different_nodes(Cluster1),
    converger_no_interleaving(Cluster1),
    converger_interleaving(Cluster1),

    ok = saturn_test_utilities:stop_datastore([1]),
    
    pass.
    
read_updates_different_nodes([Node1, Node2]) ->
    lager:info("Test started: read_updates_different_nodes"),

    Bucket=1,
    BKey={Bucket, key1},

    %% Reading a key thats empty
    {ok, {_,Clock1}} = Result1=rpc:call(Node1, saturn_leaf, read, [BKey, 0]),
    ?assertMatch({ok, {empty, Clock1}}, Result1),

    %% Update key
    Result2=rpc:call(Node1, saturn_leaf, update, [BKey, 3, Clock1]),
    ?assertMatch({ok, _Clock2}, Result2),

    %% Read from other client/node
    Result3=rpc:call(Node2, saturn_leaf, read, [BKey, 0]),
    ?assertMatch({ok, {3, _Clock2}}, Result3),

    %% Update from other client/node
    Result4=rpc:call(Node2, saturn_leaf, update, [BKey, 5, 0]),
    ?assertMatch({ok, _Clock3}, Result4),

    Result5=rpc:call(Node1, saturn_leaf, read, [BKey, Clock1]),
    ?assertMatch({ok, {5, _Clock3}}, Result5).

converger_no_interleaving([Node1, Node2])->
    lager:info("Test started: converger_no_interleaving"),

    BKey={2, key2},
    
    %% First label then data
    %% Simulate remote arrival of label {Key, Clock, Node}
    Label1 = #label{operation=update, bkey=BKey, timestamp=11, node=node2},
    Result1 = rpc:call(Node2, saturn_leaf_converger, handle, [0, {new_stream, [Label1], 1}]),
    ?assertMatch(ok, Result1),
    
    %% Should not read pending update
    Result2=rpc:call(Node1, saturn_leaf, read, [BKey, 0]),
    ?assertMatch({ok, {empty, 0}}, Result2),

    %% Simulate remote arrival of update, to complete remote label
    Result3 = rpc:call(Node2, saturn_data_receiver, data, [{saturn_data_receiver, Node1}, {11, node2}, BKey, 10]),
    ?assertMatch(ok, Result3),

    %% Should read remote update
    Result4 = saturn_test_utilities:eventual_read(BKey, Node2, 10, 0),
    ?assertMatch({ok, {10, _Clock}}, Result4),

    %% First data then label
    %% Simulate remote arrival of update, to complete remote label
    Label2 = #label{operation=update, bkey=BKey, timestamp=15, node=node3},
    Result5 = rpc:call(Node2, saturn_data_receiver, data, [{saturn_data_receiver, Node1}, {15, node3}, BKey, 20]),
    ?assertMatch(ok, Result5),

    %% Should not read pending update
    Result6=rpc:call(Node1, saturn_leaf, read, [BKey, 0]),
    ?assertMatch({ok, {10, _Clock2}}, Result6),

    %% Simulate remote arrival of label {Key, Clock, Node}
    Result7 = rpc:call(Node2, saturn_leaf_converger, handle, [0, {new_stream, [Label2], 1}]),
    ?assertMatch(ok, Result7),

    %% Should read remote update
    Result8 = saturn_test_utilities:eventual_read(BKey, Node2, 20, 0),
    ?assertMatch({ok, {20, _Clock3}}, Result8).

converger_interleaving([Node1, Node2])->
    lager:info("Test started: converger_interleaving"),

    BKey={3, key3},
    
    %% First label then data
    %% Simulate remote arrival of label {Key, Clock, Node}
    Label1 = #label{operation=update, bkey=BKey, timestamp=11, node=node2},
    Label2 = #label{operation=update, bkey=BKey, timestamp=20, node=node3},

    Result1 = rpc:call(Node2, saturn_leaf_converger, handle, [0, {new_stream, [Label1], 1}]),
    ?assertMatch(ok, Result1),
    
    %% Should not read pending update
    Result2=rpc:call(Node1, saturn_leaf, read, [BKey, 0]),
    ?assertMatch({ok, {empty, 0}}, Result2),

    %% Simulate remote arrival of update, to complete remote label
    Result3 = rpc:call(Node2, saturn_data_receiver, data, [{saturn_data_receiver, Node1}, {20, node3}, BKey, 30]),
    ?assertMatch(ok, Result3),

    %% Should not read pending update
    Result4=rpc:call(Node1, saturn_leaf, read, [BKey, 0]),
    ?assertMatch({ok, {empty, 0}}, Result4),

    %% Simulate remote arrival of update, to complete remote label
    Result5 = rpc:call(Node2, saturn_data_receiver, data, [{saturn_data_receiver, Node1}, {11, node2}, BKey, 20]),
    ?assertMatch(ok, Result5),

    %% Should read remote update
    Result6 = saturn_test_utilities:eventual_read(BKey, Node2, 20, 0),
    ?assertMatch({ok, {20, _Clock1}}, Result6),

    %% Simulate remote arrival of label {Key, Clock, Node}
    Result7 = rpc:call(Node2, saturn_leaf_converger, handle, [0, {new_stream, [Label2], 1}]),
    ?assertMatch(ok, Result7),

    %% Should read remote update
    Result8 = saturn_test_utilities:eventual_read(BKey, Node2, 30, 0),
    ?assertMatch({ok, {30, _Clock2}}, Result8).
