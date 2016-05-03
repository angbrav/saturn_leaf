-module(groups_manager_serv2).
-behaviour(gen_server).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).
-export([set_treedict/2,
         set_groupsdict/1]).

start_link() ->
    gen_server:start({local, ?MODULE}, ?MODULE, [], []).

set_treedict(Dict, NLeaves) ->
    gen_server:call(?MODULE, {set_treedict, Dict, NLeaves}, infinity).

set_groupsdict(Dict) ->
    gen_server:call(?MODULE, {set_groupsdict, Dict}, infinity).
    
init([]) ->
    {ok, empty_state}.

handle_call({set_treedict, Tree, Leaves}, _From, S0) ->
    Paths = path_from_tree_dict(Tree, Leaves),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    GrossPrefLists = riak_core_ring:all_preflists(Ring, 1),
    lists:foreach(fun(PrefList) ->
                    ok = saturn_proxy_vnode:set_tree(hd(PrefList), Paths, Tree, Leaves)
                  end, GrossPrefLists),
    {reply, ok, S0};

handle_call({set_groupsdict, RGroups}, _From, S0) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    GrossPrefLists = riak_core_ring:all_preflists(Ring, 1),
    lists:foreach(fun(PrefList) ->
                    ok = saturn_proxy_vnode:set_groups(hd(PrefList), RGroups)
                  end, GrossPrefLists),
    {reply, ok, S0}.

handle_cast(_Info, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%internal
path_from_tree_dict(Tree, Leaves) ->
    lists:foldl(fun({Id, Row}, Paths0) ->
                    case (Id >= Leaves) of
                        true ->
                            {OneHopPath, _} = lists:foldl(fun(Elem, {Acc, C}) ->
                                                            case Elem of
                                                                -1 ->
                                                                    {Acc, C+1};
                                                                _ ->
                                                                    {Acc ++ [C], C+1}
                                                             end
                                                           end, {[], 0}, Row),
                            dict:store(Id, OneHopPath, Paths0);
                        false ->
                            Paths0
                    end
                end, dict:new(), dict:to_list(Tree)).

-ifdef(TEST).

path_from_tree_dict_test() ->
    P1 = dict:store(0,[-1,1,2,3,-1],dict:new()),
    P2 = dict:store(1,[4,-1,5,6,-1],P1),
    P3 = dict:store(2,[7,8,-1,-1,9],P2),
    P4 = dict:store(3,[10,11,-1,-1,12],P3),
    P5 = dict:store(4,[-1,-1,13,14,-1],P4),
    Paths = path_from_tree_dict(P5, 3),
    ?assertEqual(2,length(dict:fetch_keys(Paths))),
    ?assertEqual([0,1,4],dict:fetch(3, Paths)),
    ?assertEqual([2,3],dict:fetch(4, Paths)).

-endif.
