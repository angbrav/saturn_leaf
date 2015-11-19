-module(one_leaf_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    NumVNodes = rt_config:get(num_vnodes, 8),
    rt:update_app_config(all,[
        {riak_core, [{ring_creation_size, NumVNodes}]}
    ]),
    N = 2,
    [Cluster1] = [[Node1, _Node2]] = rt:build_clusters([N]),

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
    ClientId1 = client1,
    ClientId2 = client2,

    %% Reading a key thats empty
    Result1=rpc:call(Node1, saturn_leaf, read, [ClientId1, Key]),
    ?assertMatch({ok, empty}, Result1),

    %% Update key
    Result2=rpc:call(Node1, saturn_leaf, update, [ClientId1, Key, 3]),
    ?assertMatch(ok, Result2),

    rpc:call(Node1, saturn_leaf, read, [ClientId1, Key]),

    %% Read from other client/node
    Result3=rpc:call(Node2, saturn_leaf, read, [ClientId2, Key]),
    ?assertMatch({ok, 3}, Result3),

    %% Update from other client/node
    Result4=rpc:call(Node2, saturn_leaf, update, [ClientId2, Key, 5]),
    ?assertMatch(ok, Result4),

    rpc:call(Node2, saturn_leaf, read, [ClientId2, Key]),

    Result5=rpc:call(Node1, saturn_leaf, read, [ClientId1, Key]),
    ?assertMatch({ok, 5}, Result5).

converger_no_interleaving([Node1, Node2])->
    lager:info("Test started: converger_no_interleaving"),

    Key=2,
    ClientId1 = client3,
    ClientId2 = client4,
    
    %% First label then data
    %% Simulate remote arrival of label {Key, Clock, Node}
    Label1 = {Key, 11, node2},
    Result1 = rpc:call(Node2, saturn_leaf_converger, handle, [0, {new_stream, [Label1], 1}]),
    ?assertMatch(ok, Result1),
    
    %% Should not read pending update
    Result2=rpc:call(Node1, saturn_leaf, read, [ClientId1, Key]),
    ?assertMatch({ok, empty}, Result2),

    %% Simulate remote arrival of update, to complete remote label
    Result3 = rpc:call(Node2, saturn_leaf_converger, handle, [0, {new_operation, Label1, Key, 10}]),
    ?assertMatch(ok, Result3),

    %% Should read remote update
    Result4 = eventual_read(ClientId2, Key, Node2, 10),
    ?assertMatch({ok, 10}, Result4),

    %% First data then label
    %% Simulate remote arrival of update, to complete remote label
    Label2 = {Key, 15, node3},
    Result5 = rpc:call(Node2, saturn_leaf_converger, handle, [0, {new_operation, Label2, Key, 20}]),
    ?assertMatch(ok, Result5),

    %% Should not read pending update
    Result6=rpc:call(Node1, saturn_leaf, read, [ClientId1, Key]),
    ?assertMatch({ok, 10}, Result6),

    %% Simulate remote arrival of label {Key, Clock, Node}
    Result7 = rpc:call(Node2, saturn_leaf_converger, handle, [0, {new_stream, [Label2], 1}]),
    ?assertMatch(ok, Result7),

    %% Should read remote update
    Result8 = eventual_read(ClientId2, Key, Node2, 20),
    ?assertMatch({ok, 20}, Result8).

converger_interleaving([Node1, Node2])->
    lager:info("Test started: converger_interleaving"),

    Key=3,
    ClientId1 = client5,
    ClientId2 = client6,
    
    %% First label then data
    %% Simulate remote arrival of label {Key, Clock, Node}
    Label1 = {Key, 11, node2},
    Label2 = {Key, 20, node3},

    Result1 = rpc:call(Node2, saturn_leaf_converger, handle, [0, {new_stream, [Label1], 1}]),
    ?assertMatch(ok, Result1),
    
    %% Should not read pending update
    Result2=rpc:call(Node1, saturn_leaf, read, [ClientId1, Key]),
    ?assertMatch({ok, empty}, Result2),

    %% Simulate remote arrival of update, to complete remote label
    Result3 = rpc:call(Node2, saturn_leaf_converger, handle, [0, {new_operation, Label2, Key, 30}]),
    ?assertMatch(ok, Result3),

    %% Should not read pending update
    Result4=rpc:call(Node1, saturn_leaf, read, [ClientId1, Key]),
    ?assertMatch({ok, empty}, Result4),

    %% Simulate remote arrival of update, to complete remote label
    Result5 = rpc:call(Node2, saturn_leaf_converger, handle, [0, {new_operation, Label1, Key, 20}]),
    ?assertMatch(ok, Result5),

    %% Should read remote update
    Result6 = eventual_read(ClientId2, Key, Node2, 20),
    ?assertMatch({ok, 20}, Result6),

    %% Simulate remote arrival of label {Key, Clock, Node}
    Result7 = rpc:call(Node2, saturn_leaf_converger, handle, [0, {new_stream, [Label2], 1}]),
    ?assertMatch(ok, Result7),

    %% Should read remote update
    Result8 = eventual_read(ClientId2, Key, Node2, 30),
    ?assertMatch({ok, 30}, Result8).

eventual_read(ClientId, Key, Node, ExpectedResult) ->
    Result=rpc:call(Node, saturn_leaf, read, [ClientId, Key]),
    case Result of
        {ok, ExpectedResult} -> Result;
        _ ->
            lager:info("I read: ~p, expecting: ~p",[Result, ExpectedResult]),
            timer:sleep(500),
            eventual_read(ClientId, Key, Node, ExpectedResult)
    end.

