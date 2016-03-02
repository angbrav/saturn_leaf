-module(three_nodes_eventual_test).

-export([confirm/0,
         three_sequential_writes_test/3]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    NumVNodes = rt_config:get(num_vnodes, 8),
    rt:update_app_config(all,[
        {riak_core, [{ring_creation_size, NumVNodes}]}
    ]),
    Clusters = [Cluster1, Cluster2, Cluster3] = rt:build_clusters([1, 1, 1]),

    lager:info("Waiting for ring to converge."),
    rt:wait_until_ring_converged(Cluster1),
    rt:wait_until_ring_converged(Cluster2),
    rt:wait_until_ring_converged(Cluster3),

    Leaf1 = hd(Cluster1),
    Leaf2 = hd(Cluster2),
    Leaf3 = hd(Cluster3),

    pong = rpc:call(Leaf1, net_adm, ping, [Leaf2]),
    pong = rpc:call(Leaf1, net_adm, ping, [Leaf3]),
    pong = rpc:call(Leaf2, net_adm, ping, [Leaf3]),

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
  
    Tree0 = dict:store(0, [-1, 300, 80], dict:new()),
    Tree1 = dict:store(1, [300, -1, 70], Tree0),
    Tree2 = dict:store(2, [80, 70, -1], Tree1),

    Groups0 = dict:store(1, [0, 1, 2], dict:new()),
    Groups1 = dict:store(2, [0, 1, 2], Groups0),
    Groups2 = dict:store(3, [0, 1], Groups1),

    ok = common_rt:set_tree_clusters(Clusters, Tree2, 2),
    ok = common_rt:set_groups_clusters(Clusters, Groups2), 

    ok = common_rt:new_node_cluster(Cluster1, 1, HostPortLeaf2),
    ok = common_rt:new_node_cluster(Cluster1, 2, HostPortLeaf3),

    ok = common_rt:new_node_cluster(Cluster2, 0, HostPortLeaf1),
    ok = common_rt:new_node_cluster(Cluster2, 2, HostPortLeaf3),

    ok = common_rt:new_node_cluster(Cluster3, 0, HostPortLeaf1),
    ok = common_rt:new_node_cluster(Cluster3, 1, HostPortLeaf2),

    three_sequential_writes_test(Leaf1, Leaf2, Leaf3),
    remote_read_test(Leaf1, Leaf2, Leaf3),

    pass.
    
three_sequential_writes_test(Leaf1, Leaf2, Leaf3) ->
    lager:info("Test started: three_sequential_writes_test"),

    BKey = {1, key1},

    %% Reading a key thats empty
    Result1=rpc:call(Leaf1, saturn_leaf, read, [BKey, clock]),
    ?assertMatch({ok, {empty,_}}, Result1),

    %% First write and subsequent reads

    %% Update key
    Result2=rpc:call(Leaf1, saturn_leaf, update, [BKey, 1, clock]),
    ?assertMatch({ok,_}, Result2),

    Result3 = saturn_test_utilities:eventual_read(BKey, Leaf1, 1),
    ?assertMatch({ok, {1,_}}, Result3),

    Result4 = saturn_test_utilities:eventual_read(BKey, Leaf2, 1),
    ?assertMatch({ok, {1,_}}, Result4),

    Result5 = saturn_test_utilities:eventual_read(BKey, Leaf3, 1),
    ?assertMatch({ok, {1,_}}, Result5),

    %% Second write and subsequent reads

    %% Update key
    Result6=rpc:call(Leaf2, saturn_leaf, update, [BKey, 2, clock]),
    ?assertMatch({ok,_}, Result6),

    Result7 = saturn_test_utilities:eventual_read(BKey, Leaf2, 2),
    ?assertMatch({ok, {2,_}}, Result7),

    Result8 = saturn_test_utilities:eventual_read(BKey, Leaf1, 2),
    ?assertMatch({ok, {2,_}}, Result8),

    Result9 = saturn_test_utilities:eventual_read(BKey, Leaf3, 2),
    ?assertMatch({ok, {2,_}}, Result9),

    %% Third write and subsequent reads

    %% Update key
    Result10=rpc:call(Leaf3, saturn_leaf, update, [BKey, 3, clock]),
    ?assertMatch({ok,_}, Result10),

    Result11 = saturn_test_utilities:eventual_read(BKey, Leaf3, 3),
    ?assertMatch({ok, {3,_}}, Result11),

    Result12 = saturn_test_utilities:eventual_read(BKey, Leaf1, 3),
    ?assertMatch({ok, {3,_}}, Result12),

    Result13 = saturn_test_utilities:eventual_read(BKey, Leaf2, 3),
    ?assertMatch({ok, {3,_}}, Result13).

remote_read_test(Leaf1, _Leaf2, Leaf3) ->
    lager:info("Test started: remote_read_test"),

    BKey = {3, key2},

    %% Reading a key thats empty
    Result1=rpc:call(Leaf1, saturn_leaf, read, [BKey, clock]),
    ?assertMatch({ok, {empty,_}}, Result1),

    %% Update key
    Result2=rpc:call(Leaf1, saturn_leaf, update, [BKey, 1, clock]),
    ?assertMatch({ok,_}, Result2),

    Result3 = saturn_test_utilities:eventual_read(BKey, Leaf3, 1),
    ?assertMatch({ok, {1,_}}, Result3).
