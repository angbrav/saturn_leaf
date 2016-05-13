-module(three_nodes_cure_test).

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
    _Clusters = [Cluster1, Cluster2, Cluster3] = rt:build_clusters([1, 1, 1]),

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

    %% Starting leaf1
    {ok, _HostPortLeaf1}=rpc:call(Leaf1, saturn_leaf_sup, start_leaf, [4040, 0]),
    %% Starting leaf2
    {ok, _HostPortLeaf2}=rpc:call(Leaf2, saturn_leaf_sup, start_leaf, [4041, 1]),
    %% Starting leaf3
    {ok, _HostPortLeaf3}=rpc:call(Leaf3, saturn_leaf_sup, start_leaf, [4042, 2]),

    ok=rpc:call(Leaf1, saturn_leaf_receiver, assign_convergers, [0, 3]),
    ok=rpc:call(Leaf2, saturn_leaf_receiver, assign_convergers, [1, 3]),
    ok=rpc:call(Leaf3, saturn_leaf_receiver, assign_convergers, [2, 3]),

    Tree0 = dict:store(0, [-1, 300, 80], dict:new()),
    Tree1 = dict:store(1, [300, -1, 70], Tree0),
    Tree2 = dict:store(2, [80, 70, -1], Tree1),

    Groups0 = dict:store(1, [0, 1, 2], dict:new()),
    Groups1 = dict:store(2, [0, 1, 2], Groups0),
    Groups2 = dict:store(3, [0], Groups1),

    ok = rpc:call(Leaf1, saturn_leaf_receiver, set_tree, [0, Tree2, 3]),
    ok = rpc:call(Leaf1, saturn_leaf_receiver, set_groups, [0, Groups2]),

    ok = rpc:call(Leaf2, saturn_leaf_receiver, set_tree, [1, Tree2, 3]),
    ok = rpc:call(Leaf2, saturn_leaf_receiver, set_groups, [1, Groups2]),

    ok = rpc:call(Leaf3, saturn_leaf_receiver, set_tree, [2, Tree2, 3]),
    ok = rpc:call(Leaf3, saturn_leaf_receiver, set_groups, [2, Groups2]),

    sequential_writes_test(Leaf1, Leaf2, Leaf3),
    remote_read_test(Leaf1, Leaf2, Leaf3),
    remote_update_test(Leaf1, Leaf2, Leaf3),

    pass.

merge(V1, V2) ->
    lists:foldl(fun({Entry, Clock2}, Acc) ->
                    Clock1 = dict:fetch(Entry, Acc),
                    dict:store(Entry, max(Clock1, Clock2), Acc)
                end, V1, dict:to_list(V2)).
    
sequential_writes_test(Leaf1, Leaf2, Leaf3) ->
    lager:info("Test started: sequential_writes_test"),

    ClockInit0 = dict:store(0, 0, dict:new()),
    ClockInit1 = dict:store(1, 0, ClockInit0),
    ClockInit = dict:store(2, 0, ClockInit1),

    BKey = {1, key1},

    %% Reading a key thats empty
    {ok, {_, C1_VC0}} = Result1=rpc:call(Leaf1, saturn_leaf, read, [BKey, ClockInit]),
    ?assertMatch({ok, {empty, C1_VC0}}, Result1),

    C1_VC2 = merge(ClockInit, C1_VC0),

    %% First write and subsequent reads

    %% Update key
    {ok, C1_VC3} = Result2=rpc:call(Leaf1, saturn_leaf, update, [BKey, 1, C1_VC2]),
    ?assertMatch({ok,_}, Result2),

    {ok, {_, C1_VC4}} = Result3 = rpc:call(Leaf1, saturn_leaf, read, [BKey, C1_VC3]),
    ?assertMatch({ok, {1,_}}, Result3),
    C1_VC5 = merge(C1_VC3, C1_VC4),

    {ok, {_, C2_VC0}} = Result4 = saturn_test_utilities:eventual_read(BKey, Leaf2, 1, ClockInit),
    ?assertMatch({ok, {1,_}}, Result4),

    {ok, {_, C3_VC0}} = Result5 = saturn_test_utilities:eventual_read(BKey, Leaf3, 1, ClockInit),
    ?assertMatch({ok, {1,_}}, Result5),

    %% Second write and subsequent reads

    %% Update key
    {ok, C2_VC1} = Result6=rpc:call(Leaf2, saturn_leaf, update, [BKey, 2, C2_VC0]),
    ?assertMatch({ok,_}, Result6),

    {ok, {_, C2_VC2}} = Result7 = rpc:call(Leaf2, saturn_leaf, read, [BKey, C2_VC1]),
    ?assertMatch({ok, {2,_}}, Result7),
    C2_VC3 = merge(C2_VC1, C2_VC2),

    {ok, {_, C1_VC6}} = Result8 = saturn_test_utilities:eventual_read(BKey, Leaf1, 2, C1_VC5),
    ?assertMatch({ok, {2,_}}, Result8),
    C1_VC7 = merge(C1_VC5, C1_VC6),

    {ok, {_, C3_VC1}} = Result9 = saturn_test_utilities:eventual_read(BKey, Leaf3, 2, C3_VC0),
    ?assertMatch({ok, {2,_}}, Result9),
    C3_VC2 = merge(C3_VC0, C3_VC1),

    %% Third write and subsequent reads

    %% Update key
    {ok, C3_VC3} = Result10=rpc:call(Leaf3, saturn_leaf, update, [BKey, 3, C3_VC2]),
    ?assertMatch({ok,_}, Result10),

    {ok, {_, C3_VC4}} = Result11 = rpc:call(Leaf3, saturn_leaf, read, [BKey, C3_VC3]),
    ?assertMatch({ok, {3,_}}, Result11),
    _C3_VC5 = merge(C3_VC3, C3_VC4),

    {ok, {_, C1_VC8}} = Result12 = saturn_test_utilities:eventual_read(BKey, Leaf1, 3, C1_VC7),
    ?assertMatch({ok, {3,_}}, Result12),
    _C1_VC9 = merge(C1_VC7, C1_VC8),

    {ok, {_, _C2_VC4}} = Result13 = saturn_test_utilities:eventual_read(BKey, Leaf2, 3, C2_VC3),
    ?assertMatch({ok, {3,_}}, Result13).

remote_read_test(Leaf1, _Leaf2, Leaf3) ->
    lager:info("Test started: remote_read_test"),

    BKeyAll = {1, test2},
    BKeyPartial = {3, test2},

    ClockInit0 = dict:store(0, 0, dict:new()),
    ClockInit1 = dict:store(1, 0, ClockInit0),
    ClockInit = dict:store(2, 0, ClockInit1),

    %% Reading a key thats empty
    {ok, {_, C1_VC0}} = Result1=rpc:call(Leaf1, saturn_leaf, read, [BKeyAll, ClockInit]),
    ?assertMatch({ok, {empty,_}}, Result1),
    C1_VC1 = merge(ClockInit, C1_VC0),

    %% Update key
    {ok, C1_VC2} = Result2=rpc:call(Leaf1, saturn_leaf, update, [BKeyAll, 1, C1_VC1]),
    ?assertMatch({ok, _}, Result2),
    
    {ok, {_, C3_VC0}} = Result3 = saturn_test_utilities:eventual_read(BKeyAll, Leaf3, 1, ClockInit),
    ?assertMatch({ok, {1,_}}, Result3),
    C3_VC1 = merge(ClockInit, C3_VC0),

    {ok, _C1_VC3}=Result4=rpc:call(Leaf1, saturn_leaf, update, [BKeyPartial, 3, C1_VC2]),
    ?assertMatch({ok, _}, Result4),

    Result5 =rpc:call(Leaf3, saturn_leaf, read, [BKeyPartial, C3_VC1]),
    ?assertMatch({ok, {3,_}}, Result5).

remote_update_test(Leaf1, Leaf2, Leaf3) ->
    lager:info("Test started: remote_update_test"),

    BKeyAll = {1, test3},
    BKeyPartial = {3, test3},

    ClockInit0 = dict:store(0, 0, dict:new()),
    ClockInit1 = dict:store(1, 0, ClockInit0),
    ClockInit = dict:store(2, 0, ClockInit1),

    %% Reading a key thats empty
    {ok, {_, C1_VC0}} = Result1=rpc:call(Leaf1, saturn_leaf, read, [BKeyAll, ClockInit]),
    ?assertMatch({ok, {empty,_}}, Result1),
    C1_VC1 = merge(ClockInit, C1_VC0),

    %% Update key
    {ok, _C1_VC2} = Result2=rpc:call(Leaf1, saturn_leaf, update, [BKeyAll, 1, C1_VC1]),
    ?assertMatch({ok, _}, Result2),
   
    {ok, {_, C3_VC0}} = Result3 = saturn_test_utilities:eventual_read(BKeyAll, Leaf3, 1, ClockInit),
    ?assertMatch({ok, {1,_}}, Result3),
    C3_VC1 = merge(ClockInit, C3_VC0),
    
    Result4=rpc:call(Leaf3, saturn_leaf, update, [BKeyPartial, 3, C3_VC1]),
    ?assertMatch({ok, _}, Result4),

    Result5 = saturn_test_utilities:eventual_read(BKeyPartial, Leaf2, 3, ClockInit),
    ?assertMatch({ok, {3,_}}, Result5).
