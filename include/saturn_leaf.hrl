-define(PRINT(Var), io:format("DEBUG: ~p:~p - ~p~n~n ~p~n~n", [?MODULE, ?LINE, ??Var, Var])).

-define(BUCKET, <<"saturn_leaf">>).

-define(PROXY_MASTER, saturn_proxy_vnode_master).
-define(PROXY_SERVICE, saturn_proxy).
-define(SIMPLE_MASTER, saturn_simple_backend_vnode_master).
-define(SIMPLE_SERVICE, saturn_simple_backend).

-define(GROUPSFILE, "data/manager/groups_file_simple.saturn").
-define(TREEFILE, "data/manager/tree_file_simple.saturn").
-define(TREEFILE_TEST, "../include/tree_file_test.saturn").
-define(GROUPSFILE_TEST, "../include/groups_file_test.saturn").

-define(BACKEND, simple_backend).
-define(BACKEND_CONNECTOR, simple_backend_connector).

-define(MYIDPREFIX, {prefix, myid_prefix}).
-define(MYIDKEY, myid_key).

-define(HEARTBEAT_FREQ, 1000).

-record(label, {operation :: remote_read | update | remote_reply,
                key,
                timestamp :: non_neg_integer(),
                node,
                sender :: non_neg_integer(),
                payload}).

-record(payload_reply, {to :: all | non_neg_integer(),
                        client,
                        value
                       }).

-record(payload_remote, {to :: all | non_neg_integer(),
                         key_source,
                         client
                        }).
