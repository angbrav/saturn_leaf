-module(common_rt).
-export([assign_id_cluster/2,
         new_node_cluster/3,
         set_tree_clusters/3,
         set_groups_clusters/2,
         get_uname/1
        ]).

get_uname({Host, Port}) ->
    Host ++ integer_to_list(Port).

assign_id_cluster([], _Id) ->
    ok;

assign_id_cluster([Node|T], Id) ->
    rpc:call(Node, groups_manager_serv, set_myid, [Id]),
    assign_id_cluster(T, Id).

new_node_cluster([], _Id, _HostPort) ->
    ok;

new_node_cluster([Node|T], Id, HostPort) ->
    rpc:call(Node, groups_manager_serv, new_node, [Id, HostPort]),
    new_node_cluster(T, Id, HostPort).

set_tree_clusters([], _Tree, _Leaves) ->
    ok;

set_tree_clusters([Cluster|T], Tree, Leaves) ->
    lists:foreach(fun(Node) ->
                    rpc:call(Node, groups_manager_serv, set_treedict, [Tree, Leaves])
                  end, Cluster),
    set_tree_clusters(T, Tree, Leaves).
    
set_groups_clusters([], _Groups) ->
    ok;

set_groups_clusters([Cluster|T], Groups) ->
    lists:foreach(fun(Node) ->
                    rpc:call(Node, groups_manager_serv, set_groupsdict, [Groups])
                  end, Cluster),
    set_groups_clusters(T, Groups).
