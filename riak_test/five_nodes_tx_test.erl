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
-module(five_nodes_tx_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    ok = saturn_test_utilities:clean_datastore_data([1,2,3]),

    NumVNodes = rt_config:get(num_vnodes, 8),
    rt:update_app_config(all,[
        {riak_core, [{ring_creation_size, NumVNodes}]}
    ]),
    _Clusters = [Cluster1, Cluster2, Cluster3, Cluster4, Cluster5] = rt:build_clusters([1, 1, 1, 1, 1]),


    lager:info("Waiting for ring to converge."),
    rt:wait_until_ring_converged(Cluster1),
    rt:wait_until_ring_converged(Cluster2),
    rt:wait_until_ring_converged(Cluster3),
    rt:wait_until_ring_converged(Cluster4),
    rt:wait_until_ring_converged(Cluster5),

    Leaf1 = hd(Cluster1),
    Leaf2 = hd(Cluster2),
    Leaf3 = hd(Cluster3),
    Internal1 = hd(Cluster4),
    Internal2 = hd(Cluster5),

    pong = rpc:call(Leaf1, net_adm, ping, [Leaf2]),
    pong = rpc:call(Leaf1, net_adm, ping, [Leaf3]),
    pong = rpc:call(Leaf1, net_adm, ping, [Internal1]),

    pong = rpc:call(Leaf2, net_adm, ping, [Leaf3]),
    pong = rpc:call(Leaf2, net_adm, ping, [Internal1]),

    pong = rpc:call(Internal1, net_adm, ping, [Internal2]),

    pong = rpc:call(Leaf3, net_adm, ping, [Internal2]),

    pong = net_adm:ping(Leaf1),
    pong = net_adm:ping(Leaf2),
    pong = net_adm:ping(Leaf3),

    rt:wait_for_service(Leaf1, saturn_proxy),
    rt:wait_for_service(Leaf2, saturn_proxy),
    rt:wait_for_service(Leaf3, saturn_proxy),

    %% Starting leaf1
    {ok, _HostPortLeaf1}=rpc:call(Leaf1, saturn_leaf_sup, start_leaf, [4040, 0]),
    %% Starting leaf2
    {ok, _HostPortLeaf2}=rpc:call(Leaf2, saturn_leaf_sup, start_leaf, [4041, 1]),
    %% Starting leaf3
    {ok, _HostPortLeaf3}=rpc:call(Leaf3, saturn_leaf_sup, start_leaf, [4042, 2]),

    ok=rpc:call(Leaf1, saturn_leaf_producer, check_ready, [0]),
    ok=rpc:call(Leaf2, saturn_leaf_producer, check_ready, [1]),
    ok=rpc:call(Leaf3, saturn_leaf_producer, check_ready, [2]),

    ok=rpc:call(Leaf1, saturn_leaf_receiver, assign_convergers, [0, 3]),
    ok=rpc:call(Leaf2, saturn_leaf_receiver, assign_convergers, [1, 3]),
    ok=rpc:call(Leaf3, saturn_leaf_receiver, assign_convergers, [2, 3]),

    %% Starting internal1
    {ok, _HostPortInternal1}=rpc:call(Internal1, saturn_leaf_sup, start_internal, [4043, 3]),
    %% Starting internal2
    {ok, _HostPortInternal2}=rpc:call(Internal2, saturn_leaf_sup, start_internal, [4044, 4]),

    Tree0 = dict:store(0, [-1,1,2,3,-1], dict:new()),
    Tree1 = dict:store(1, [4,-1,5,6,-1], Tree0),
    Tree2 = dict:store(2, [7,8,-1,-1,9], Tree1),
    Tree3 = dict:store(3, [10,11,-1,-1,12], Tree2),
    Tree4 = dict:store(4, [-1,-1,13,14,-1], Tree3),

    Groups0 = dict:store(1, [0, 1, 2], dict:new()),
    Groups1 = dict:store(2, [0, 1, 2], Groups0),
    Groups2 = dict:store(3, [0, 1], Groups1),

    ok = rpc:call(Leaf1, saturn_leaf_producer, set_tree, [0, Tree4, 3]),
    ok = rpc:call(Leaf1, saturn_leaf_producer, set_groups, [0, Groups2]),

    ok = rpc:call(Leaf2, saturn_leaf_producer, set_tree, [1, Tree4, 3]),
    ok = rpc:call(Leaf2, saturn_leaf_producer, set_groups, [1, Groups2]),

    ok = rpc:call(Leaf3, saturn_leaf_producer, set_tree, [2, Tree4, 3]),
    ok = rpc:call(Leaf3, saturn_leaf_producer, set_groups, [2, Groups2]),

    ok = rpc:call(Internal1, saturn_internal_serv, set_tree, [3, Tree4, 3]),
    ok = rpc:call(Internal1, saturn_internal_serv, set_groups, [3, Groups2]),

    ok = rpc:call(Internal2, saturn_internal_serv, set_tree, [4, Tree4, 3]),
    ok = rpc:call(Internal2, saturn_internal_serv, set_groups, [4, Groups2]),

    five_nodes_test(Leaf1, Leaf2, Leaf3),
    remote_read_async_test(Leaf1, Leaf3),
    writetx_local_test(Leaf1, Leaf3),
    writetx_remote_read_test(Leaf1, Leaf3),

    ok = saturn_test_utilities:stop_datastore([1,2,3]),

    pass.

five_nodes_test(Leaf1, Leaf2, _Leaf3) ->
    lager:info("Test started: five_nodes_async_test"),

    BKey1={1, key1},
    BKey2={1, key2},
    BKey3={1, key3},
    
    %% Reading a key thats empty
    Result1=gen_server:call(saturn_test_utilities:server_name(Leaf1), {read, BKey1, 0}, infinity),
    %Result1=rpc:call(Leaf1, saturn_leaf, read, [BKey, 0]),
    ?assertMatch({ok, {empty, 0}}, Result1),

    %% Update key
    {_, Clock1} = Result2 = gen_server:call(saturn_test_utilities:server_name(Leaf1), {update, BKey1, 1, 0}, infinity),
    ?assertMatch({ok, _}, Result2),

    {_, Clock2} = Result3 = gen_server:call(saturn_test_utilities:server_name(Leaf1), {update, BKey2, 2, Clock1}, infinity),
    ?assertMatch({ok, _}, Result3),

    {_, _Clock3} = Result4 = gen_server:call(saturn_test_utilities:server_name(Leaf1), {update, BKey3, 3, Clock2}, infinity),
    ?assertMatch({ok, _}, Result4),

    {_, {_, Clock4}} = Result5 = saturn_test_utilities:eventual_da_read(BKey3, Leaf2, 3),
    ?assertMatch({ok, {3, _}}, Result5),
    
    {ok, {Values, _}} = gen_server:call(saturn_test_utilities:server_name(Leaf2), {read_tx, [BKey1, BKey2, BKey3], Clock4}, infinity),
    ValuesSorted = lists:sort(Values),
    ExpectedSorted = lists:sort([{BKey1, 1}, {BKey2, 2}, {BKey3, 3}]),
    ?assertMatch(ExpectedSorted, ValuesSorted).

remote_read_async_test(Leaf1, Leaf3) ->
    lager:info("Test started: remote_reads_async"),
    
    BKey1={1, key4},
    BKey2={3, key5},
    BKey3={1, key6},
    
    %% Reading a key thats empty
    Result1=gen_server:call(saturn_test_utilities:server_name(Leaf1), {read, BKey1, 0}, infinity),
    %Result1=rpc:call(Leaf1, saturn_leaf, read, [BKey, 0]),
    ?assertMatch({ok, {empty, 0}}, Result1),

    %% Update key
    {_, Clock1} = Result2 = gen_server:call(saturn_test_utilities:server_name(Leaf1), {update, BKey1, 1, 0}, infinity),
    ?assertMatch({ok, _}, Result2),

    {_, Clock2} = Result3 = gen_server:call(saturn_test_utilities:server_name(Leaf1), {update, BKey2, 2, Clock1}, infinity),
    ?assertMatch({ok, _}, Result3),

    {_, _Clock3} = Result4 = gen_server:call(saturn_test_utilities:server_name(Leaf1), {update, BKey3, 3, Clock2}, infinity),
    ?assertMatch({ok, _}, Result4),

    {_, {_, Clock4}} = Result5 = saturn_test_utilities:eventual_da_read(BKey3, Leaf3, 3),
    ?assertMatch({ok, {3, _}}, Result5),
    
    {ok, {Values, _}} = gen_server:call(saturn_test_utilities:server_name(Leaf3), {read_tx, [BKey1, BKey2, BKey3], Clock4}, infinity),
    ValuesSorted = lists:sort(Values),
    ExpectedSorted = lists:sort([{BKey1, 1}, {BKey2, 2}, {BKey3, 3}]),
    ?assertMatch(ExpectedSorted, ValuesSorted).

writetx_local_test(Leaf1, Leaf3) ->
    lager:info("Test started: writetx_local_test"),
    
    BKey1={1, key7},
    BKey2={1, key8},
    BKey3={1, key9},

    {ok, {Values, _}} = gen_server:call(saturn_test_utilities:server_name(Leaf1), {read_tx, [BKey1, BKey2, BKey3], 0}, infinity),
    ValuesSorted = lists:sort(Values),
    ExpectedSorted = lists:sort([{BKey1, empty}, {BKey2, empty}, {BKey3, empty}]),
    ?assertMatch(ExpectedSorted, ValuesSorted),

    {ok, _Clock0} = gen_server:call(saturn_test_utilities:server_name(Leaf1), {write_tx, [{BKey1, 1}, {BKey2, 2}, {BKey3, 3}], 0}, infinity),

    {_, {_, Clock1}} = Result1 = saturn_test_utilities:eventual_da_read(BKey1, Leaf3, 1),
    ?assertMatch({ok, {1, _}}, Result1),

    {ok, {Values1, _}} = gen_server:call(saturn_test_utilities:server_name(Leaf3), {read_tx, [BKey1, BKey2, BKey3], Clock1}, infinity),
    ValuesSorted1 = lists:sort(Values1),
    ExpectedSorted1 = lists:sort([{BKey1, 1}, {BKey2, 2}, {BKey3, 3}]),
    ?assertMatch(ExpectedSorted1, ValuesSorted1).

writetx_remote_read_test(Leaf1, Leaf3) ->
    lager:info("Test started: writetx_remote_read_test"),

    BKey1={1, key10},
    BKey2={3, key11},
    BKey3={1, key12},

    {ok, {Values, _}} = gen_server:call(saturn_test_utilities:server_name(Leaf1), {read_tx, [BKey1, BKey2, BKey3], 0}, infinity),
    ValuesSorted = lists:sort(Values),
    ExpectedSorted = lists:sort([{BKey1, empty}, {BKey2, empty}, {BKey3, empty}]),
    ?assertMatch(ExpectedSorted, ValuesSorted),

    {ok, _Clock0} = gen_server:call(saturn_test_utilities:server_name(Leaf1), {write_tx, [{BKey1, 1}, {BKey2, 2}, {BKey3, 3}], 0}, infinity),

    {_, {_, Clock1}} = Result1 = saturn_test_utilities:eventual_da_read(BKey1, Leaf3, 1),
    ?assertMatch({ok, {1, _}}, Result1),

    {ok, {Values1, _}} = gen_server:call(saturn_test_utilities:server_name(Leaf3), {read_tx, [BKey1, BKey2, BKey3], Clock1}, infinity),
    ValuesSorted1 = lists:sort(Values1),
    ExpectedSorted1 = lists:sort([{BKey1, 1}, {BKey2, 2}, {BKey3, 3}]),
    ?assertMatch(ExpectedSorted1, ValuesSorted1).
