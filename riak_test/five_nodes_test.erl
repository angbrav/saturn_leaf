-module(five_nodes_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
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
    Groups1 = dict:store(2, [0, 1, 2], Groups0),
    Groups2 = dict:store(3, [0, 1, 2], Groups1),

    ok = common_rt:set_tree_clusters(Clusters, Tree4, 3),
    ok = common_rt:set_groups_clusters(Clusters, Groups2),

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

    five_nodes_test(Leaf1, Leaf2, Leaf3),

    pass.
    
five_nodes_test(Leaf1, Leaf2, Leaf3) ->
    lager:info("Test started: five_nodes_test"),

    Key=1,
    ClientId1 = client1,
    ClientId2 = client2,
    ClientId3 = client3,

    %% Reading a key thats empty
    Result1=rpc:call(Leaf1, saturn_leaf, read, [ClientId1, Key]),
    ?assertMatch({ok, empty}, Result1),

    %% Update key
    Result2=rpc:call(Leaf1, saturn_leaf, update, [ClientId1, Key, 3]),
    ?assertMatch(ok, Result2),

    Result3=rpc:call(Leaf1, saturn_leaf, read, [ClientId1, Key]),
    ?assertMatch({ok, 3}, Result3),

    Result4 = eventual_read(ClientId2, Key, Leaf2, 3),
    ?assertMatch({ok, 3}, Result4),

    Result5 = eventual_read(ClientId3, Key, Leaf3, 3),
    ?assertMatch({ok, 3}, Result5).

eventual_read(ClientId, Key, Node, ExpectedResult) ->
    Result=rpc:call(Node, saturn_leaf, read, [ClientId, Key]),
    case Result of
        {ok, ExpectedResult} -> Result;
        _ ->
            lager:info("I read: ~p, expecting: ~p",[Result, ExpectedResult]),
            timer:sleep(500),
            eventual_read(ClientId, Key, Node, ExpectedResult)
    end.

