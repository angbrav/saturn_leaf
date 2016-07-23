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
-module(two_leaf_one_internal_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    ok = saturn_test_utilities:clean_datastore_data([1,2]),

    NumVNodes = rt_config:get(num_vnodes, 8),
    rt:update_app_config(all,[
        {riak_core, [{ring_creation_size, NumVNodes}]}
    ]),
    rt:update_app_config(all,[
        {saturn_leaf, [{groups, "data/manager/groups.saturn"}]}
    ]),
    rt:update_app_config(all,[
        {saturn_leaf, [{tree, "data/manager/tree.saturn"}]}
    ]),
    _Clusters = [Cluster1, Cluster2, Cluster3] = rt:build_clusters([1, 1, 1]),

    lager:info("Waiting for ring to converge."),
    rt:wait_until_ring_converged(Cluster1),
    rt:wait_until_ring_converged(Cluster2),
    rt:wait_until_ring_converged(Cluster3),

    Leaf1 = hd(Cluster1),
    Leaf2 = hd(Cluster2),
    Internal1 = hd(Cluster3),

    pong = rpc:call(Leaf1, net_adm, ping, [Leaf2]),
    pong = rpc:call(Leaf1, net_adm, ping, [Internal1]),
    pong = rpc:call(Leaf2, net_adm, ping, [Internal1]),

    rt:wait_for_service(Leaf1, saturn_proxy),
    rt:wait_for_service(Leaf2, saturn_proxy),

    %% Starting leaf1
    {ok, _HostPortLeaf1}=rpc:call(Leaf1, saturn_leaf_sup, start_leaf, [4040, 0]),
    %% Starting leaf2
    {ok, _HostPortLeaf2}=rpc:call(Leaf2, saturn_leaf_sup, start_leaf, [4041, 1]),

    ok=rpc:call(Leaf1, saturn_leaf_producer, check_ready, [0]),
    ok=rpc:call(Leaf2, saturn_leaf_producer, check_ready, [1]),

    ok=rpc:call(Leaf1, saturn_leaf_receiver, assign_convergers, [0, 2]),
    ok=rpc:call(Leaf2, saturn_leaf_receiver, assign_convergers, [1, 2]),
    
    %% Starting internal1
    {ok, _HostPortInternal1}=rpc:call(Internal1, saturn_leaf_sup, start_internal, [4042, 2]),

    Tree0 = dict:store(0, [-1, 300, 50], dict:new()),
    Tree1 = dict:store(1, [300, -1, 70], Tree0),
    Tree2 = dict:store(2, [50, 70, -1], Tree1),

    Groups0 = dict:store(1, [0, 1], dict:new()),
    Groups1 = dict:store(2, [0, 1], Groups0),
    Groups2 = dict:store(3, [0, 1], Groups1),
    
    ok = rpc:call(Leaf1, saturn_leaf_producer, set_tree, [0, Tree2, 2]),
    ok = rpc:call(Leaf1, saturn_leaf_producer, set_groups, [0, Groups2]),

    ok = rpc:call(Leaf2, saturn_leaf_producer, set_tree, [1, Tree2, 2]),
    ok = rpc:call(Leaf2, saturn_leaf_producer, set_groups, [1, Groups2]),

    ok = rpc:call(Internal1, saturn_internal_serv, set_tree, [2, Tree2, 2]),
    ok = rpc:call(Internal1, saturn_internal_serv, set_groups, [2, Groups2]),

    full_setup_test(Leaf1, Leaf2),

    ok = saturn_test_utilities:stop_datastore([1,2]),

    pass.
    
full_setup_test(Leaf1, Leaf2) ->
    lager:info("Test started: full_setup_test"),

    BKey={1, key1},

    %% Reading a key thats empty
    Result1=rpc:call(Leaf1, saturn_leaf, read, [BKey, 0]),
    ?assertMatch({ok, {empty, 0}}, Result1),

    %% Update key
    Result2=rpc:call(Leaf1, saturn_leaf, update, [BKey, 3, 0]),
    ?assertMatch({ok, _Clock1}, Result2),

    Result3=rpc:call(Leaf1, saturn_leaf, read, [BKey, 0]),
    ?assertMatch({ok, {3, _Clock1}}, Result3),

    Result4 = saturn_test_utilities:eventual_read(BKey, Leaf2, 3),
    ?assertMatch({ok, {3, _Clock1}}, Result4).
