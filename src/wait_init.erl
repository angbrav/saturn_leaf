-module(wait_init).

-export([wait_ready_nodes/1,
	 check_ready/1]).

%% @doc This function takes a list of pysical nodes connected to the an
%% instance of the antidote distributed system.  For each of the phyisical nodes
%% it checks if all of the vnodes have been initialized, meaning ets tables
%% and gen_servers serving read have been started.
%% Returns true if all vnodes are initialized for all phyisical nodes,
%% false otherwise
wait_ready_nodes([]) ->
    true;
wait_ready_nodes([Node|Rest]) ->
    case check_ready(Node) of
	true ->
	    wait_ready_nodes(Rest);
	false ->
	    false
    end.

%% @doc This function provides the same functionality as wait_ready_nodes
%% except it takes as input a single physical node instead of a list
check_ready(Node) ->
    lager:info("Checking if node ~w is ready ~n", [Node]),
    case rpc:call(Node,saturn_proxy_vnode,check_ready,[{check_myid_ready}]) of
	true ->
        lager:info("Node ~w is ready! ~n", [Node]),
        true;
	false ->
	    lager:info("Checking if node ~w is ready ~n", [Node]),
	    false
    end.

    
