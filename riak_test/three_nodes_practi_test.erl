-module(three_nodes_practi_test).

-export([confirm/0,
         sequential_writes_test/3,
         remote_update_test/3,
         remote_read_test/3]).

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

    rt:wait_for_service(Leaf1, saturn_proxy),
    rt:wait_for_service(Leaf2, saturn_proxy),
    rt:wait_for_service(Leaf3, saturn_proxy),
    
    Tree0 = dict:store(0, [-1, 300, 80], dict:new()),
    Tree1 = dict:store(1, [300, -1, 70], Tree0),
    Tree2 = dict:store(2, [80, 70, -1], Tree1),

    Groups0 = dict:store(1, [0, 1, 2], dict:new()),
    Groups1 = dict:store(2, [0, 1, 2], Groups0),
    Groups2 = dict:store(3, [0, 1], Groups1),
    Groups3 = dict:store(4, [0], Groups2),

    ok = common_rt:set_tree_clusters(Clusters, Tree2, 2),
    ok = common_rt:set_groups_clusters(Clusters, Groups3), 
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
  
    ok = common_rt:new_node_cluster(Cluster1, 1, HostPortLeaf2),
    ok = common_rt:new_node_cluster(Cluster1, 2, HostPortLeaf3),

    ok = common_rt:new_node_cluster(Cluster2, 0, HostPortLeaf1),
    ok = common_rt:new_node_cluster(Cluster2, 2, HostPortLeaf3),

    ok = common_rt:new_node_cluster(Cluster3, 0, HostPortLeaf1),
    ok = common_rt:new_node_cluster(Cluster3, 1, HostPortLeaf2),

    sequential_writes_test(Leaf1, Leaf2, Leaf3),
    remote_read_test(Leaf1, Leaf2, Leaf3),
    remote_update_test(Leaf1, Leaf2, Leaf3),

    pass.
    
sequential_writes_test(Leaf1, Leaf2, Leaf3) ->
    lager:info("Test started: sequential_writes_test"),

    BKey = {1, key1},

    %% Reading a key thats empty
    Result1=rpc:call(Leaf1, saturn_leaf, read, [BKey, null]),
    ?assertMatch({ok, empty}, Result1),

    %% First write and subsequent reads

    %% Update key
    Result2=rpc:call(Leaf1, saturn_leaf, update, [BKey, 1, null]),
    ?assertMatch(ok, Result2),

    Result3 = rpc:call(Leaf1, saturn_leaf, read, [BKey, null]),
    ?assertMatch({ok, 1}, Result3),

    Result4 = saturn_test_utilities:eventual_read(BKey, Leaf2, 1, null),
    ?assertMatch({ok, 1}, Result4),

    Result5 = saturn_test_utilities:eventual_read(BKey, Leaf3, 1, null),
    ?assertMatch({ok, 1}, Result5),

    %% Second write and subsequent reads

    %% Update key
    Result6=rpc:call(Leaf2, saturn_leaf, update, [BKey, 2, null]),
    ?assertMatch(ok, Result6),

    Result7 = rpc:call(Leaf2, saturn_leaf, read, [BKey, null]),
    ?assertMatch({ok, 2}, Result7),

    Result8 = saturn_test_utilities:eventual_read(BKey, Leaf1, 2, null),
    ?assertMatch({ok, 2}, Result8),

    Result9 = saturn_test_utilities:eventual_read(BKey, Leaf3, 2, null),
    ?assertMatch({ok, 2}, Result9),

    %% Third write and subsequent reads

    %% Update key
    Result10=rpc:call(Leaf3, saturn_leaf, update, [BKey, 3, null]),
    ?assertMatch(ok, Result10),

    Result11 = rpc:call(Leaf3, saturn_leaf, read, [BKey, null]),
    ?assertMatch({ok, 3}, Result11),

    Result12 = saturn_test_utilities:eventual_read(BKey, Leaf1, 3, null),
    ?assertMatch({ok, 3}, Result12),

    Result13 = saturn_test_utilities:eventual_read(BKey, Leaf2, 3, null),
    ?assertMatch({ok, 3}, Result13).

remote_read_test(Leaf1, _Leaf2, Leaf3) ->
    lager:info("Test started: remote_read_test"),

    BKeyAll = {1, test2},
    BKeyPartial = {4, test2},

    %% Reading a key thats empty
    Result1=rpc:call(Leaf1, saturn_leaf, read, [BKeyAll, null]),
    ?assertMatch({ok, empty}, Result1),

    %% Update key
    Result2=rpc:call(Leaf1, saturn_leaf, update, [BKeyAll, 1, null]),
    ?assertMatch(ok, Result2),
    
    Result3 = saturn_test_utilities:eventual_read(BKeyAll, Leaf3, 1, null),
    ?assertMatch({ok, 1}, Result3),

    Result4=rpc:call(Leaf1, saturn_leaf, update, [BKeyPartial, 4, null]),
    ?assertMatch(ok, Result4),

    Result5=rpc:call(Leaf3, saturn_leaf, read, [BKeyPartial, null]),
    ?assertMatch({ok, 4}, Result5).

remote_update_test(Leaf1, Leaf2, Leaf3) ->
    lager:info("Test started: remote_update_test"),

    BKeyAll = {1, test3},
    BKeyPartial = {3, test3},

    %% Reading a key thats empty
    Result1=rpc:call(Leaf1, saturn_leaf, read, [BKeyAll, null]),
    ?assertMatch({ok, empty}, Result1),

    %% Update key
    Result2=rpc:call(Leaf1, saturn_leaf, update, [BKeyAll, 1, null]),
    ?assertMatch(ok, Result2),

    Result3 = saturn_test_utilities:eventual_read(BKeyAll, Leaf3, 1, null),
    ?assertMatch({ok, 1}, Result3),

    Result4=rpc:call(Leaf3, saturn_leaf, update, [BKeyPartial, 3, null]),
    ?assertMatch(ok, Result4),

    Result5 = saturn_test_utilities:eventual_read(BKeyPartial, Leaf2, 3, null),
    ?assertMatch({ok, 3}, Result5).
