-module(two_leaf_one_internal_test).

-export([confirm/0]).

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
    Internal1 = hd(Cluster3),

    %% Starting leaf1
    {ok, HostPortLeaf1}=rpc:call(Leaf1, saturn_leaf_sup, start_leaf, [4040, 0]),
    %% Starting leaf2
    {ok, HostPortLeaf2}=rpc:call(Leaf2, saturn_leaf_sup, start_leaf, [4041, 1]),

    lager:info("Waiting until vnodes are started up"),
    rt:wait_until(hd(Cluster1),fun wait_init:check_ready/1),
    rt:wait_until(hd(Cluster2),fun wait_init:check_ready/1),
    lager:info("Vnodes are started up"),
    
    Tree0 = dict:store(0, [-1, 300, 50], dict:new()),
    Tree1 = dict:store(1, [300, -1, 70], Tree0),
    Tree2 = dict:store(2, [50, 70, -1], Tree1),

    Groups0 = dict:store(1, [0, 1], dict:new()),
    Groups1 = dict:store(2, [0, 1], Groups0),
    Groups2 = dict:store(3, [0, 1], Groups1),

    ok = common_rt:set_tree_clusters(Clusters, Tree2, 2),
    ok = common_rt:set_groups_clusters(Clusters, Groups2),

    %% Starting internal1
    {ok, HostPortInternal1}=rpc:call(Internal1, saturn_internal_sup, start_internal, [4042, 2]),

    ok = common_rt:new_node_cluster(Cluster1, 1, HostPortLeaf2),
    ok = common_rt:new_node_cluster(Cluster1, 2, HostPortInternal1),

    ok = common_rt:new_node_cluster(Cluster2, 0, HostPortLeaf1),
    ok = common_rt:new_node_cluster(Cluster2, 2, HostPortInternal1),

    ok = common_rt:new_node_cluster(Cluster3, 0, HostPortLeaf1),
    ok = common_rt:new_node_cluster(Cluster3, 1, HostPortLeaf2),

    full_setup_test(Leaf1, Leaf2),

    pass.
    
full_setup_test(Leaf1, Leaf2) ->
    lager:info("Test started: full_setup_test"),

    Key=1,
    ClientId1 = client1,
    ClientId2 = client2,

    %% Reading a key thats empty
    Result1=rpc:call(Leaf1, saturn_leaf, read, [ClientId1, Key]),
    ?assertMatch({ok, empty}, Result1),

    %% Update key
    Result2=rpc:call(Leaf1, saturn_leaf, update, [ClientId1, Key, 3]),
    ?assertMatch(ok, Result2),

    Result3=rpc:call(Leaf1, saturn_leaf, read, [ClientId1, Key]),
    ?assertMatch({ok, 3}, Result3),

    Result4 = eventual_read(ClientId2, Key, Leaf2, 3),
    ?assertMatch({ok, 3}, Result4).

eventual_read(ClientId, Key, Node, ExpectedResult) ->
    Result=rpc:call(Node, saturn_leaf, read, [ClientId, Key]),
    case Result of
        {ok, ExpectedResult} -> Result;
        _ ->
            lager:info("I read: ~p, expecting: ~p",[Result, ExpectedResult]),
            timer:sleep(500),
            eventual_read(ClientId, Key, Node, ExpectedResult)
    end.
