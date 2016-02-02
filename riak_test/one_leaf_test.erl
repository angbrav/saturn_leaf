-module(one_leaf_test).

-export([confirm/0]).

-export([read_updates_different_nodes/1,
         converger_no_interleaving/1,
         converger_interleaving/1]).

-include("saturn_leaf.hrl").

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    NumVNodes = rt_config:get(num_vnodes, 8),
    rt:update_app_config(all,[
        {riak_core, [{ring_creation_size, NumVNodes}]}
    ]),
    N = 2,
    [Cluster1] = [[Node1, _Node2]] = rt:build_clusters([N]),

    saturn_test_utilities:clean_datastore_data(),

    lager:info("Waiting for ring to converge."),
    rt:wait_until_ring_converged(Cluster1),

    %% Starting servers in one node (Node1)
    {ok, _HostPort}=rpc:call(Node1, saturn_leaf_sup, start_leaf, [4040, 0]),

    lager:info("Waiting until vnodes are started up"),
    rt:wait_until(hd(Cluster1),fun wait_init:check_ready/1),
    lager:info("Vnodes are started up"),

    read_updates_different_nodes(Cluster1),
    converger_no_interleaving(Cluster1),
    converger_interleaving(Cluster1),

    pass.
    
read_updates_different_nodes([Node1, Node2]) ->
    lager:info("Test started: read_updates_different_nodes"),

    Key=1,

    %% Reading a key thats empty
    {ok, {_,Clock1}} = Result1=rpc:call(Node1, saturn_leaf, read, [Key, 0]),
    ?assertMatch({ok, {empty, Clock1}}, Result1),

    %% Update key
    Result2=rpc:call(Node1, saturn_leaf, update, [Key, 3, Clock1]),
    ?assertMatch({ok, _Clock2}, Result2),

    %% Read from other client/node
    Result3=rpc:call(Node2, saturn_leaf, read, [Key, 0]),
    ?assertMatch({ok, {3, _Clock2}}, Result3),

    %% Update from other client/node
    Result4=rpc:call(Node2, saturn_leaf, update, [Key, 5, 0]),
    ?assertMatch({ok, _Clock3}, Result4),

    Result5=rpc:call(Node1, saturn_leaf, read, [Key, Clock1]),
    ?assertMatch({ok, {5, _Clock3}}, Result5).

converger_no_interleaving([Node1, Node2])->
    lager:info("Test started: converger_no_interleaving"),

    Key=2,
    
    %% First label then data
    %% Simulate remote arrival of label {Key, Clock, Node}
    Label1 = #label{operation=update, key=Key, timestamp=11, node=node2},
    Result1 = rpc:call(Node2, saturn_leaf_converger, handle, [0, {new_stream, [Label1], 1}]),
    ?assertMatch(ok, Result1),
    
    %% Should not read pending update
    Result2=rpc:call(Node1, saturn_leaf, read, [Key, 0]),
    ?assertMatch({ok, {empty, 0}}, Result2),

    %% Simulate remote arrival of update, to complete remote label
    Result3 = rpc:call(Node2, saturn_leaf_converger, handle, [0, {new_operation, Label1, 10}]),
    ?assertMatch(ok, Result3),

    %% Should read remote update
    Result4 = saturn_test_utilities:eventual_read(Key, Node2, 10, 0),
    ?assertMatch({ok, {10, _Clock}}, Result4),

    %% First data then label
    %% Simulate remote arrival of update, to complete remote label
    Label2 = #label{operation=update, key=Key, timestamp=15, node=node3},
    Result5 = rpc:call(Node2, saturn_leaf_converger, handle, [0, {new_operation, Label2, 20}]),
    ?assertMatch(ok, Result5),

    %% Should not read pending update
    Result6=rpc:call(Node1, saturn_leaf, read, [Key, 0]),
    ?assertMatch({ok, {10, _Clock2}}, Result6),

    %% Simulate remote arrival of label {Key, Clock, Node}
    Result7 = rpc:call(Node2, saturn_leaf_converger, handle, [0, {new_stream, [Label2], 1}]),
    ?assertMatch(ok, Result7),

    %% Should read remote update
    Result8 = saturn_test_utilities:eventual_read(Key, Node2, 20, 0),
    ?assertMatch({ok, {20, _Clock3}}, Result8).

converger_interleaving([Node1, Node2])->
    lager:info("Test started: converger_interleaving"),

    Key=3,
    
    %% First label then data
    %% Simulate remote arrival of label {Key, Clock, Node}
    Label1 = #label{operation=update, key=Key, timestamp=11, node=node2},
    Label2 = #label{operation=update, key=Key, timestamp=20, node=node3},

    Result1 = rpc:call(Node2, saturn_leaf_converger, handle, [0, {new_stream, [Label1], 1}]),
    ?assertMatch(ok, Result1),
    
    %% Should not read pending update
    Result2=rpc:call(Node1, saturn_leaf, read, [Key, 0]),
    ?assertMatch({ok, {empty, 0}}, Result2),

    %% Simulate remote arrival of update, to complete remote label
    Result3 = rpc:call(Node2, saturn_leaf_converger, handle, [0, {new_operation, Label2, 30}]),
    ?assertMatch(ok, Result3),

    %% Should not read pending update
    Result4=rpc:call(Node1, saturn_leaf, read, [Key, 0]),
    ?assertMatch({ok, {empty, 0}}, Result4),

    %% Simulate remote arrival of update, to complete remote label
    Result5 = rpc:call(Node2, saturn_leaf_converger, handle, [0, {new_operation, Label1, 20}]),
    ?assertMatch(ok, Result5),

    %% Should read remote update
    Result6 = saturn_test_utilities:eventual_read(Key, Node2, 20, 0),
    ?assertMatch({ok, {20, _Clock1}}, Result6),

    %% Simulate remote arrival of label {Key, Clock, Node}
    Result7 = rpc:call(Node2, saturn_leaf_converger, handle, [0, {new_stream, [Label2], 1}]),
    ?assertMatch(ok, Result7),

    %% Should read remote update
    Result8 = saturn_test_utilities:eventual_read(Key, Node2, 30, 0),
    ?assertMatch({ok, {30, _Clock2}}, Result8).
