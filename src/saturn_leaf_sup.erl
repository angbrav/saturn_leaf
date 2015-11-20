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

    riak_core_metadata:put(?MYIDPREFIX, ?MYIDKEY, MyId),

    supervisor:start_child(?MODULE, {saturn_leaf_converger,
                    {saturn_leaf_converger, start_link, [MyId]},
                    permanent, 5000, worker, [saturn_leaf_converger]}),

    supervisor:start_child(?MODULE, {saturn_leaf_tcp_recv_fsm,
                    {saturn_leaf_tcp_recv_fsm, start_link, [Port, saturn_leaf_converger, MyId]},
                    permanent, 5000, worker, [saturn_leaf_tcp_recv_fsm]}),

    supervisor:start_child(?MODULE, {saturn_leaf_tcp_connection_handler_fsm_sup,
                    {saturn_leaf_tcp_connection_handler_fsm_sup, start_link, []},
                    permanent, 5000, supervisor, [saturn_leaf_tcp_connection_handler_fsm_sup]}),

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
    Childs0 = [ProxyMaster, PropagatorSup],
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
