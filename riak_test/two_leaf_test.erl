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
-module(two_leaf_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    ok = saturn_test_utilities:clean_datastore_data([1,2]),

    NumVNodes = rt_config:get(num_vnodes, 8),
    rt:update_app_config(all,[
        {riak_core, [{ring_creation_size, NumVNodes}]}
    ]),
    Clusters = [Cluster1, Cluster2] = rt:build_clusters([1, 1]),

    lager:info("Waiting for ring to converge."),
    rt:wait_until_ring_converged(Cluster1),
    rt:wait_until_ring_converged(Cluster2),

    Node1 = hd(Cluster1),
    Node2 = hd(Cluster2),

    pong = rpc:call(Node1, net_adm, ping, [Node2]),

    rt:wait_for_service(Node1, saturn_proxy),
    rt:wait_for_service(Node2, saturn_proxy),

    Tree0 = dict:store(0, [-1, 300, 50], dict:new()),
    Tree1 = dict:store(1, [300, -1, 70], Tree0),
    Tree2 = dict:store(2, [50, 70, -1], Tree1),

    Groups0 = dict:store(1, [0, 1], dict:new()),
    Groups1 = dict:store(2, [0, 1], Groups0),
    Groups2 = dict:store(3, [0, 1], Groups1),

    ok = common_rt:set_tree_clusters(Clusters, Tree2, 2),
    ok = common_rt:set_groups_clusters(Clusters, Groups2),

    %% Starting servers in Node1
    {ok, HostPort0}=rpc:call(Node1, saturn_leaf_sup, start_leaf, [4040, 0]),
    
    %% Starting servers in Node1
    {ok, HostPort1}=rpc:call(Node2, saturn_leaf_sup, start_leaf, [4041, 1]),

    ok=rpc:call(Node1, saturn_leaf_producer, check_ready, [0]),
    ok=rpc:call(Node2, saturn_leaf_producer, check_ready, [1]),

    ok = common_rt:new_node_cluster(Cluster1, 1, HostPort1),
    ok = common_rt:new_node_cluster(Cluster2, 0, HostPort0),

    communication_between_leafs(hd(Cluster1), hd(Cluster2)),

    ok = saturn_test_utilities:stop_datastore([1,2]),

    pass.
    
communication_between_leafs(Node1, Node2) ->
    lager:info("Test started: communication_between_leafs"),

    BKey={1, key1},

    %% Reading a key thats empty
    Result1=rpc:call(Node1, saturn_leaf, read, [BKey, 0]),
    ?assertMatch({ok, {empty, 0}}, Result1),

    %% Update key
    Result2=rpc:call(Node1, saturn_leaf, update, [BKey, 3, 0]),
    ?assertMatch({ok, _Clock1}, Result2),

    Result3=rpc:call(Node1, saturn_leaf, read, [BKey, 0]),
    ?assertMatch({ok, {3, _Clock2}}, Result3),

    {ok, Label1} = rpc:call(Node1, saturn_proxy_vnode, last_label, [BKey]),

    %% Read from other client/node
    Result4=rpc:call(Node2, saturn_leaf, read, [BKey, 0]),
    ?assertMatch({ok, {empty, 0}}, Result4),

    Result5 = rpc:call(Node2, saturn_leaf_converger, handle, [1, {new_stream, [Label1], 2}]),
    ?assertMatch(ok, Result5),

    Result6 = saturn_test_utilities:eventual_read(BKey, Node2, 3),
    ?assertMatch({ok, {3, _Clock3}}, Result6).
