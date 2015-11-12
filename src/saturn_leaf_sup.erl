-module(saturn_leaf_sup).

-behaviour(supervisor).
-include("saturn_leaf.hrl").
%% API
-export([start_link/0,
         start_leaf/1]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_leaf(Port) ->

    supervisor:start_child(?MODULE, {saturn_leaf_converger,
                    {saturn_leaf_converger, start_link, []},
                    permanent, 5000, worker, [saturn_leaf_converger]}),

    supervisor:start_child(?MODULE, {saturn_tcp_recv_fsm,
                    {saturn_tcp_recv_fsm, start_link, [Port, saturn_leaf_converger]},
                    permanent, 5000, worker, [saturn_tcp_recv_fsm]}),

    supervisor:start_child(?MODULE, {tcp_connection_handler_fsm_sup,
                    {tcp_connection_handler_fsm_sup, start_link, []},
                    permanent, 5000, supervisor, [tcp_connection_handler_fsm_sup]}),

    supervisor:start_child(?MODULE, {saturn_leaf_producer,
                    {saturn_leaf_producer, start_link, []},
                    permanent, 5000, worker, [saturn_leaf_producer]}).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init(_Args) ->
    ProxyMaster = {?PROXY_MASTER,
                  {riak_core_vnode_master, start_link, [saturn_proxy_vnode]},
                  permanent, 5000, worker, [riak_core_vnode_master]},
    PropagatorSup = {propagation_fsm_sup,
                    {propagation_fsm_sup, start_link, []},
                    permanent, 5000, supervisor, [propagation_fsm_sup]},
    GroupsManager = {saturn_groups_manager,
                     {saturn_groups_manager, start_link, []},
                     permanent, 5000, worker, [saturn_groups_manager]},
    Childs0 = [ProxyMaster, PropagatorSup, GroupsManager],
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
