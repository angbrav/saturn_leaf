-module(common_rt).
-export([assign_id_cluster/2
        ]).

assign_id_cluster([], _Id) ->
    ok;

assign_id_cluster([Node|T], Id) ->
    rpc:call(Node, saturn_groups_manager, set_myid, [0]),
    assign_id_cluster(T, Id).
