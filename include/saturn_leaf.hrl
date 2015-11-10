-define(PRINT(Var), io:format("DEBUG: ~p:~p - ~p~n~n ~p~n~n", [?MODULE, ?LINE, ??Var, Var])).
-define(PROXY_MASTER, saturn_proxy_vnode_master).
-define(PROXY_SERVICE, saturn_proxy).
-define(SIMPLE_MASTER, saturn_simple_backend_vnode_master).
-define(SIMPLE_SERVICE, saturn_simple_backend).

-define(GROUPSFILE, whatever).
-define(TREEFILE, whatever).

-define(BACKEND, simple_backend).
-define(BACKEND_CONNECTOR_FSM, simple_backend_connector_fsm).
