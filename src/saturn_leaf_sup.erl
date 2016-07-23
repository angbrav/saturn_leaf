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
         start_leaf/2,
         start_internal/2]).

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
    supervisor:start_child(?MODULE, {saturn_leaf_receiver,
                    {saturn_leaf_receiver, start_link, [MyId]},
                    permanent, 5000, worker, [saturn_leaf_receiver]}),

    supervisor:start_child(?MODULE, {saturn_leaf_converger,
                    {saturn_leaf_converger, start_link, [MyId]},
                    permanent, 5000, worker, [saturn_leaf_converger]}),
    
    supervisor:start_child(?MODULE, {saturn_leaf_producer,
                    {saturn_leaf_producer, start_link, [MyId]},
                    permanent, 5000, worker, [saturn_leaf_producer]}),

    {ok, {Host, Port}}.

start_internal(Port, MyId) ->
    
    supervisor:start_child(?MODULE, {saturn_internal_serv,
                    {saturn_internal_serv, start_link, [MyId]},
                    permanent, 5000, worker, [saturn_internal_serv]}),
    
    {ok, List} = inet:getif(),
    {Ip, _, _} = hd(List),
    Host = inet_parse:ntoa(Ip),

    {ok, {Host, Port}}.

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init(_Args) ->
    ProxyMaster = {?PROXY_MASTER,
                  {riak_core_vnode_master, start_link, [saturn_proxy_vnode]},
                  permanent, 5000, worker, [riak_core_vnode_master]},
    ClientReceiver = {saturn_client_receiver,
                     {saturn_client_receiver, start_link, []},
                     permanent, 5000, worker, [saturn_client_receiver]},
    DataReceiver = {saturn_data_receiver,
                     {saturn_data_receiver, start_link, []},
                     permanent, 5000, worker, [saturn_data_receiver]},
    Childs0 = [ProxyMaster, ClientReceiver, DataReceiver],
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
