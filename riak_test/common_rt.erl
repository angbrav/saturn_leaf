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
                    rpc:call(Node, groups_manager_serv2, set_treedict, [Tree, Leaves])
                  end, Cluster),
    set_tree_clusters(T, Tree, Leaves).
    
set_groups_clusters([], _Groups) ->
    ok;

set_groups_clusters([Cluster|T], Groups) ->
    lists:foreach(fun(Node) ->
                    rpc:call(Node, groups_manager_serv2, set_groupsdict, [Groups])
                  end, Cluster),
    set_groups_clusters(T, Groups).
