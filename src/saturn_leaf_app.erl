-module(saturn_leaf_app).

-behaviour(application).
-include("saturn_leaf.hrl").

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    case saturn_leaf_sup:start_link() of
        {ok, Pid} ->
            ok = riak_core:register([{vnode_module, saturn_proxy_vnode}]),
            ok = riak_core_node_watcher:service_up(?PROXY_SERVICE, self()),
        
            case ?BACKEND of
                simple_backend ->
                    ok = riak_core:register([{vnode_module, saturn_simple_backend_vnode}]),
                    ok = riak_core_node_watcher:service_up(?SIMPLE_SERVICE, self());
                _ ->
                    noop
            end,

            ok = riak_core_ring_events:add_guarded_handler(saturn_leaf_ring_event_handler, []),
            ok = riak_core_node_watcher_events:add_guarded_handler(saturn_leaf_node_event_handler, []),
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

stop(_State) ->
    ok.
