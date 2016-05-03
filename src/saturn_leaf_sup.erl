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
-module(saturn_leaf_sup).

-behaviour(supervisor).
-include("saturn_leaf.hrl").
%% API
-export([start_link/0,
         start_leaf/2]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_leaf(Port, MyId) ->

    {ok, List} = inet:getif(),
    {Ip, _, _} = hd(List),
    Host = inet_parse:ntoa(Ip),

    %riak_core_metadata:put(?MYIDPREFIX, ?MYIDKEY, MyId),

    supervisor:start_child(?MODULE, {saturn_leaf_converger,
                    {saturn_leaf_converger, start_link, [MyId]},
                    permanent, 5000, worker, [saturn_leaf_converger]}),
    
    case ?PROPAGATION_MODE of
        short_tcp ->
            supervisor:start_child(?MODULE, {saturn_leaf_tcp_recv_fsm,
                    {saturn_leaf_tcp_recv_fsm, start_link, [Port, saturn_leaf_converger, MyId]},
                    permanent, 5000, worker, [saturn_leaf_tcp_recv_fsm]}),

            supervisor:start_child(?MODULE, {saturn_leaf_tcp_connection_handler_fsm_sup,
                    {saturn_leaf_tcp_connection_handler_fsm_sup, start_link, []},
                    permanent, 5000, supervisor, [saturn_leaf_tcp_connection_handler_fsm_sup]});

        _ ->
            noop
    end,
    supervisor:start_child(?MODULE, {saturn_leaf_producer,
                    {saturn_leaf_producer, start_link, [MyId]},
                    permanent, 5000, worker, [saturn_leaf_producer]}),

    {ok, {Host, Port}}.

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init(_Args) ->
    ProxyMaster = {?PROXY_MASTER,
                  {riak_core_vnode_master, start_link, [saturn_proxy_vnode]},
                  permanent, 5000, worker, [riak_core_vnode_master]},
    PropagatorSup = {saturn_leaf_propagation_fsm_sup,
                    {saturn_leaf_propagation_fsm_sup, start_link, []},
                    permanent, 5000, supervisor, [saturn_leaf_propagation_fsm_sup]},
    ClientReceiver = {saturn_client_receiver,
                     {saturn_client_receiver, start_link, []},
                     permanent, 5000, worker, [saturn_client_receiver]},
    ManagerServ = {groups_manager_serv2,
                  {groups_manager_serv2, start_link, []},
                  permanent, 5000, worker, [groups_manager_serv2]},
    Childs0 = [ProxyMaster, PropagatorSup, ClientReceiver, ManagerServ],
    Childs1 = case ?BACKEND of
                simple_backend ->
                    BackendMaster = {?SIMPLE_MASTER,
                                    {riak_core_vnode_master, start_link, [saturn_simple_backend_vnode]},
                                    permanent, 5000, worker, [riak_core_vnode_master]},
                    Childs0 ++ [BackendMaster];
                _ ->
                    Childs0
    end,
    
    { ok,
        { {one_for_one, 5, 10},
          Childs1}}.
