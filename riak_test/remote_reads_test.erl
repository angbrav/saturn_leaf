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
%% --------------------------------------------------------------------module(remote_reads_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    ok = saturn_test_utilities:clean_datastore_data([1,2,3]),

    NumVNodes = rt_config:get(num_vnodes, 8),
    rt:update_app_config(all,[
        {riak_core, [{ring_creation_size, NumVNodes}]}
    ]),
    Clusters = [Cluster1, Cluster2, Cluster3, Cluster4, Cluster5] = rt:build_clusters([1, 1, 1, 1, 1]),

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

    timer:sleep(1000),
    %% Starting leaf1
    {ok, HostPortLeaf1}=rpc:call(Leaf1, saturn_leaf_sup, start_leaf, [4040, 0]),
    %% Starting leaf2
    {ok, HostPortLeaf2}=rpc:call(Leaf2, saturn_leaf_sup, start_leaf, [4041, 1]),
    %% Starting leaf3
    {ok, HostPortLeaf3}=rpc:call(Leaf3, saturn_leaf_sup, start_leaf, [4042, 2]),

    lager:info("Waiting until vnodes are started up"),
    rt:wait_until(hd(Cluster1),fun wait_init:check_ready/1),
    rt:wait_until(hd(Cluster2),fun wait_init:check_ready/1),
    rt:wait_until(hd(Cluster3),fun wait_init:check_ready/1),
    lager:info("Vnodes are started up"),
    
    Tree0 = dict:store(0, [-1,1,2,3,-1], dict:new()),
    Tree1 = dict:store(1, [4,-1,5,6,-1], Tree0),
    Tree2 = dict:store(2, [7,8,-1,-1,9], Tree1),
    Tree3 = dict:store(3, [10,11,-1,-1,12], Tree2),
    Tree4 = dict:store(4, [-1,-1,13,14,-1], Tree3),

    Groups0 = dict:store(1, [0, 1, 2], dict:new()),
    Groups1 = dict:store(2, [0, 1], Groups0),
    Groups2 = dict:store(3, [0], Groups1),
    Groups3 = dict:store(4, [0, 1, 2], Groups2),

    ok = common_rt:set_tree_clusters(Clusters, Tree4, 3),
    ok = common_rt:set_groups_clusters(Clusters, Groups3),

    %% Starting internal1
    {ok, HostPortInternal1}=rpc:call(Internal1, saturn_internal_sup, start_internal, [4043, 3]),
    %% Starting internal2
    {ok, HostPortInternal2}=rpc:call(Internal2, saturn_internal_sup, start_internal, [4044, 4]),

    ok = common_rt:new_node_cluster(Cluster1, 1, HostPortLeaf2),
    ok = common_rt:new_node_cluster(Cluster1, 2, HostPortLeaf3),
    ok = common_rt:new_node_cluster(Cluster1, 3, HostPortInternal1),
    ok = common_rt:new_node_cluster(Cluster1, 4, HostPortInternal2),

    ok = common_rt:new_node_cluster(Cluster2, 0, HostPortLeaf1),
    ok = common_rt:new_node_cluster(Cluster2, 2, HostPortLeaf3),
    ok = common_rt:new_node_cluster(Cluster2, 3, HostPortInternal1),
    ok = common_rt:new_node_cluster(Cluster2, 4, HostPortInternal2),

    ok = common_rt:new_node_cluster(Cluster3, 0, HostPortLeaf1),
    ok = common_rt:new_node_cluster(Cluster3, 1, HostPortLeaf2),
    ok = common_rt:new_node_cluster(Cluster3, 3, HostPortInternal1),
    ok = common_rt:new_node_cluster(Cluster3, 4, HostPortInternal2),

    ok = common_rt:new_node_cluster(Cluster4, 0, HostPortLeaf1),
    ok = common_rt:new_node_cluster(Cluster4, 1, HostPortLeaf2),
    ok = common_rt:new_node_cluster(Cluster4, 2, HostPortLeaf3),
    ok = common_rt:new_node_cluster(Cluster4, 4, HostPortInternal2),

    ok = common_rt:new_node_cluster(Cluster5, 0, HostPortLeaf1),
    ok = common_rt:new_node_cluster(Cluster5, 1, HostPortLeaf2),
    ok = common_rt:new_node_cluster(Cluster5, 2, HostPortLeaf3),
    ok = common_rt:new_node_cluster(Cluster5, 3, HostPortInternal1),

    single_partial_test(Leaf1, Leaf2, Leaf3),
    multiple_partial_test(Leaf1, Leaf2, Leaf3),

    ok = saturn_test_utilities:stop_datastore([1,2,3]),

    pass.
    
single_partial_test(Leaf1, Leaf2, _Leaf3) ->
    lager:info("Test started: single_partial_test"),

    KeySingle=3,
    KeyAll=1,
    
    %% Reading a key thats empty
    Result1=rpc:call(Leaf1, saturn_leaf, read, [KeySingle, 0]),
    ?assertMatch({ok, {empty, 0}}, Result1),

    %% Update key
    Result2=rpc:call(Leaf1, saturn_leaf, update, [KeySingle, 3, 0]),
    ?assertMatch({ok, _Clock1}, Result2),

    Result3=rpc:call(Leaf1, saturn_leaf, read, [KeySingle, 0]),
    ?assertMatch({ok, {3, _Clock1}}, Result3),

    Result4=rpc:call(Leaf1, saturn_leaf, read, [KeyAll, 0]),
    ?assertMatch({ok, {empty, 0}}, Result4),

    %% Update key
    Result5=rpc:call(Leaf1, saturn_leaf, update, [KeyAll, 1, 0]),
    ?assertMatch({ok, _Clock2}, Result5),

    Result6=rpc:call(Leaf1, saturn_leaf, read, [KeyAll, 0]),
    ?assertMatch({ok, {1, _Clock2}}, Result6),

    {ok, {_Value, Clock3}} = Result7 = saturn_test_utilities:eventual_read(KeyAll, Leaf2, 1),
    ?assertMatch({ok, {1, Clock3}}, Result7),

    Result8=rpc:call(Leaf2, saturn_leaf, read, [KeySingle, Clock3]),
    ?assertMatch({ok, {3, _Clock4}}, Result8).

multiple_partial_test(Leaf1, _Leaf2, Leaf3) ->
    lager:info("Test started: single_partial_test"),

    KeyPartial=2,
    KeyAll=4,
   
    %% Reading a key thats empty
    Result1=rpc:call(Leaf1, saturn_leaf, read, [KeyPartial, 0]),
    ?assertMatch({ok, {empty, 0}}, Result1),

    %% Update key
    Result2=rpc:call(Leaf1, saturn_leaf, update, [KeyPartial, 2, 0]),
    ?assertMatch({ok, _Clock1}, Result2),

    Result3=rpc:call(Leaf1, saturn_leaf, read, [KeyPartial, 0]),
    ?assertMatch({ok, {2, _Clock1}}, Result3),

    Result4=rpc:call(Leaf1, saturn_leaf, read, [KeyAll, 0]),
    ?assertMatch({ok, {empty, 0}}, Result4),

    %% Update key
    Result5=rpc:call(Leaf1, saturn_leaf, update, [KeyAll, 4, 0]),
    ?assertMatch({ok, _Clock2}, Result5),

    Result6=rpc:call(Leaf1, saturn_leaf, read, [KeyAll, 0]),
    ?assertMatch({ok, {4, _Clock2}}, Result6),

    {ok, {_Value, Clock3}} = Result7 = saturn_test_utilities:eventual_read(KeyAll, Leaf3, 4),
    ?assertMatch({ok, {4, Clock3}}, Result7),

    Result8=rpc:call(Leaf3, saturn_leaf, read, [KeyPartial, Clock3]),
    ?assertMatch({ok, {2, _Clock4}}, Result8).
