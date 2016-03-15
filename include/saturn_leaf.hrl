-define(PRINT(Var), io:format("DEBUG: ~p:~p - ~p~n~n ~p~n~n", [?MODULE, ?LINE, ??Var, Var])).

-define(PROXY_MASTER, saturn_proxy_vnode_master).
-define(PROXY_SERVICE, saturn_proxy).
-define(SIMPLE_MASTER, saturn_simple_backend_vnode_master).
-define(SIMPLE_SERVICE, saturn_simple_backend).
-define(COPS_MASTER, saturn_cops_vnode_master).
-define(COPS_SERVICE, saturn_cops).

-define(BUCKET_COPS, <<"cops">>).

-define(GROUPSFILE, "data/manager/groups_file_simple.saturn").
-define(TREEFILE, "data/manager/tree_file_simple.saturn").
-define(TREEFILE_TEST, "../include/tree_file_test.saturn").
-define(GROUPSFILE_TEST, "../include/groups_file_test.saturn").

%SIMPLE BACKEND
%-define(BACKEND, simple_backend).
%-define(BACKEND_CONNECTOR, simple_backend_connector).

%SIMPLE OVERLAPPING_BACKEND
-define(BACKEND, simple_overlapping_backend).
-define(BACKEND_CONNECTOR, simple_overlapping_dict_backend_connector).
%cops
%-define(DEFAULT_METADATA, {0, []}).
%practi
-define(DEFAULT_METADATA, 0).

%RIAK
%-define(BACKEND, riak_backend).
%-define(BACKEND_CONNECTOR, riak_connector).

-define(MYIDPREFIX, {prefix, myid_prefix}).
-define(MYIDKEY, myid_key).

-define(HEARTBEAT_FREQ, 1000).

-define(PROPAGATION_MODE, naive_erlang).
%-define(PROPAGATION_MODE, short_tcp).

-record(label, {operation :: remote_read | update | remote_reply,
                bkey,
                timestamp :: non_neg_integer(),
                node,
                sender :: non_neg_integer(),
                payload}).

-record(payload_reply, {to :: all | non_neg_integer(),
                        client,
                        deps,
                        value
                       }).

-record(payload_remote, {to :: all | non_neg_integer(),
                         bucket_source,
                         deps,
                         client
                        }).

-record(payload_remote_update, {deps,
                                client
                               }).
