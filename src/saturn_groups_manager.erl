-module(saturn_groups_manager).
-behaviour(gen_server).

-include("saturn_leaf.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).
-export([get_datanodes/1,
         get_metadatanodes/2,
         new_node/2,
         set_myid/1,
         filter_stream_leaf/1,
         do_replicate/1]).

-record(state, {groups,
                map,
                tree,
                paths,
                myid,
                nleaves}).
                

start_link() ->
    gen_server:start({local, ?MODULE}, ?MODULE, [], []).

do_replicate(Key) ->
    gen_server:call(?MODULE, {do_replicate, Key}, infinity).

get_datanodes(Key) ->
    gen_server:call(?MODULE, {get_datanodes, Key}, infinity).

get_metadatanodes(Key, Id) ->
    gen_server:call(?MODULE, {get_metadatanodes, Key, Id}, infinity).

new_node(Id, HostPort) ->
    gen_server:call(?MODULE, {new_node, Id, HostPort}, infinity).

set_myid(MyId) ->
    gen_server:call(?MODULE, {set_myid, MyId}, infinity).

filter_stream_leaf(Stream) ->
    gen_server:call(?MODULE, {filter_stream_leaf, Stream}, infinity).
    
init([]) ->
    {ok, GroupsFile} = file:open(?GROUPSFILE, [read]),
    RGroups = replication_groups_from_file(GroupsFile, dict:new()),
    file:close(GroupsFile),
     
    {ok, TreeFile} = file:open(?TREEFILE, [read]),
    case file:read_line(TreeFile) of
        eof ->
            lager:error("Empty file: ~p", [?TREEFILE]),
            S1 = #state{groups=RGroups, paths=dict:new(), tree=dict:new(), nleaves=0};
        {error, Reason} ->
            lager:error("Problem reading ~p file, reason: ~p", [?TREEFILE, Reason]),
            S1 = #state{groups=RGroups, paths=dict:new(), tree=dict:new(), nleaves=0};
        {ok, Line} ->
            {NLeaves, []} = string:to_integer(hd(string:tokens(Line, "\n"))),
            {Tree, Paths} = tree_from_file(TreeFile, 0, NLeaves, dict:new(), dict:new()),
            S1 = #state{groups=RGroups, paths=Paths, tree=Tree, nleaves=NLeaves}
    end,
    file:close(TreeFile),
    {ok, S1#state{map=dict:new()}}.

handle_call({set_myid, MyId}, _From, S0) ->
    {reply, ok, S0#state{myid=MyId}};

handle_call({new_node, Id, HostPort}, _From, S0=#state{map=Map0}) ->
    case dict:find(Id, Map0) of
        {ok, _Value} ->
            lager:error("Node already known"),
            {reply, ok, S0};
        error ->
            Map1 = dict:store(Id, HostPort, Map0),
            {reply, ok, S0#state{map=Map1}}
    end;

handle_call({do_replicate, Key}, _From, S0=#state{groups=RGroups, myid=MyId}) ->
    case dict:find(Key, RGroups) of
        {ok, Value} ->
            case contains(MyId, Value) of
                true ->
                    {reply, true, S0};
                false ->
                    {reply, false, S0}
            end;
        error ->
            {reply, {error, unknown_key}, S0}
    end;
        
handle_call({get_datanodes, Key}, _From, S0=#state{groups=RGroups, map=Map, myid=MyId}) ->
    case dict:find(Key, RGroups) of
        {ok, Value} ->
            Group = lists:foldl(fun(Id, Acc) ->
                                    case Id of
                                        MyId ->
                                            Acc;
                                        _ ->
                                            case dict:find(Id, Map) of
                                                {ok, Value} -> Acc ++ [Value];
                                                error -> Acc
                                            end
                                    end
                                end, [], Value),
            {reply, {ok, Group}, S0};
        error ->
            {reply, {error, unknown_key}, S0}
    end;

handle_call({filter_stream_leaf, Stream0}, _From, S0=#state{tree=Tree, nleaves=NLeaves, myid=MyId, map=Map}) ->
    Row = dict:fetch(MyId, Tree),
    Internal = find_internal(Row, 0, NLeaves),
    Stream1 = lists:foldl(fun(Elem, Acc) ->
                            {Key, _Clock, _Node} = Elem,
                            case interested(Internal, Key, MyId, S0) of
                                true ->
                                    Acc ++ [Elem];
                                false ->
                                    Acc
                            end
                          end, [], Stream0),
    IndexNode = case dict:find(Internal, Map) of
                    {ok, Value} -> Value;
                    error ->
                        lager:error("The id: ~p is not in the map ~p",[Internal, dict:fetch_keys(Map)]),
                        no_indexnode
                end,
    {reply, {ok, Stream1, IndexNode}, S0};

handle_call({get_metadatanodes, Key}, _From, S0=#state{groups=_RGroups, map=Map, tree=Tree, paths=Paths, nleaves=NLeaves, myid=MyId}) ->
    Row = dict:fetch(MyId, Tree),
    case is_leaf(MyId, NLeaves) of
        true ->
            Internal = find_internal(Row, 0, NLeaves),
            case interested(Internal, Key, MyId, S0) of
                true ->
                    {reply, {ok, dict:fetch(Internal, Map)}, S0};
                false ->
                    {reply, {ok, []}, S0}
            end;
        false ->
            Links = dict:fetch(MyId, Paths),
            FilteredLinks = lists:foldl(fun(Elem, Acc) ->
                                        case Elem of
                                            MyId -> Acc;
                                            _ -> Acc ++ [Elem]
                                        end 
                                       end, [], Links),
            Group = lists:foldl(fun(Id, Acc) ->
                                    case interested(Id, Key, MyId, S0) of
                                        true ->
                                            case dict:find(Id, Map) of
                                                {ok, Value} -> Acc ++ [Value];
                                                error -> Acc
                                            end;
                                        false ->
                                            Acc
                                    end
                                end, [], FilteredLinks),
            {reply, {ok, Group}, S0}
    end.

handle_cast(_Info, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%private
interested(Id, Key, PreId, S0=#state{groups=RGroups, nleaves=NLeaves, paths=Paths}) ->
    Group = dict:fetch(Key, RGroups),
    case is_leaf(Id, NLeaves) of
        true ->
            contains(Id, Group);
        false ->
            Links = dict:fetch(Id, Paths),
            FilteredLinks = lists:foldl(fun(Elem, Acc) ->
                                        case Elem of
                                            PreId -> Acc;
                                            _ -> Acc ++ [Elem]
                                        end
                                       end, [], Links),
            LinksExpanded = expand_links(FilteredLinks, Id, S0),
            intersect(LinksExpanded, Group)
    end.
            
expand_links([], _PreId, _S0) ->
    [];
               
expand_links([H|T], PreId, S0=#state{nleaves=NLeaves, paths=Paths}) ->
    case is_leaf(H, NLeaves) of
        true ->
            [H] ++ expand_links(T, PreId, S0);
        false ->
            ExtraPath = dict:fetch(H, Paths),
            FilteredExtraPath = lists:foldl(fun(Elem, Acc) ->
                                            case Elem of
                                                PreId -> Acc;
                                                _ -> Acc ++ [Elem]
                                            end 
                                           end, [], ExtraPath),
            expand_links(FilteredExtraPath, H, S0) ++ expand_links(T, PreId, S0)
    end.

intersect([], _List2) ->
    false;

intersect(_List1=[H|T], List2) ->
    case contains(H, List2) of
        true ->
            true;
        false ->
            intersect(T, List2)
    end.

contains(_Id, []) ->
    false;

contains(Id, [H|T]) ->
    case H of
        Id ->
            true;
        _ ->
            contains(Id, T)
    end.

replication_groups_from_file(Device, Dict0)->
    case file:read_line(Device) of
        eof ->
            Dict0;
        {error, Reason} ->
            lager:error("Problem reading ~p file, reason: ~p", [?GROUPSFILE, Reason]),
            Dict0;
        {ok, Line} ->
            [H|T] = string:tokens(hd(string:tokens(Line,"\n")), ","),
            ReplicationGroup = lists:foldl(fun(Elem, Acc) ->
                                            {Int, []} = string:to_integer(Elem),
                                            Acc ++ [Int]
                                           end, [], T),
            {Key, []} = string:to_integer(H),
            Dict1 = dict:store(Key, ReplicationGroup, Dict0),
            replication_groups_from_file(Device, Dict1)
    end.

tree_from_file(Device, Counter, LeavesLeft, Tree0, Path0)->
    case file:read_line(Device) of
        eof ->
            {Tree0, Path0};
        {error, Reason} ->
            lager:error("Problem reading ~p file, reason: ~p", [?TREEFILE, Reason]),
            {Tree0, Path0};
        {ok, Line} ->
            List = string:tokens(hd(string:tokens(Line, "\n")), ","),
            Latencies = lists:foldl(fun(Elem, Acc) ->
                                            {Int, []} = string:to_integer(Elem),
                                            Acc ++ [Int]
                                           end, [], List),
            Tree1 = dict:store(Counter, Latencies, Tree0),
            case LeavesLeft of
                0 ->
                    {OneHopPath, _} = lists:foldl(fun(Elem, {Acc, C}) ->
                                                    {Int, []} = string:to_integer(Elem),
                                                    case Int of
                                                        -1 ->
                                                            {Acc, C+1};
                                                        _ ->
                                                            {Acc ++ [C], C+1}
                                                    end
                                                  end, {[], 0}, List),
                    Path1 = dict:store(Counter, OneHopPath, Path0),
                    tree_from_file(Device, Counter + 1, 0, Tree1, Path1);
                _ ->
                    tree_from_file(Device, Counter + 1, LeavesLeft - 1, Tree1, Path0)
            end
    end.

is_leaf(Id, Total) ->
    Id<Total.
      
find_internal([H|T], Counter, NLeaves) ->
    case (Counter<NLeaves) of
        true ->
            find_internal(T, Counter+1, NLeaves);
        false ->
            case H>=0 of
                true ->
                    Counter;
                false ->
                    find_internal(T, Counter+1, NLeaves)
            end
    end.

-ifdef(TEST).
interested_test() ->
    P1 = dict:store(6,[0,1,7],dict:new()),
    P2 = dict:store(7,[2,6,10],P1),
    P3 = dict:store(8,[3,9,10],P2),
    P4 = dict:store(9,[4,5,8],P3),
    P5 = dict:store(10,[7,8],P4),
    Groups = dict:store(3, [0,1,3], dict:new()),
    ?assertEqual(true, interested(0, 3, 6, #state{groups=Groups, nleaves=6, paths=P5})),
    ?assertEqual(false, interested(2, 3, 7, #state{groups=Groups, nleaves=6, paths=P5})),
    ?assertEqual(true, interested(7, 3, 10, #state{groups=Groups, nleaves=6, paths=P5})),
    ?assertEqual(false, interested(9, 3, 8, #state{groups=Groups, nleaves=6, paths=P5})).
    

expand_links_test() ->
    P1 = dict:store(6,[0,1,7],dict:new()),
    P2 = dict:store(7,[2,6,10],P1),
    P3 = dict:store(8,[3,9,10],P2),
    P4 = dict:store(9,[4,5,8],P3),
    P5 = dict:store(10,[7,8],P4),
    Result1 = expand_links([6], 0, #state{nleaves=6, paths=P5}),
    ?assertEqual([1,2,3,4,5], lists:sort(Result1)),
    Result2 = expand_links([7], 2, #state{nleaves=6, paths=P5}),
    ?assertEqual([0,1,3,4,5], lists:sort(Result2)),
    Result3 = expand_links([7,8], 10, #state{nleaves=6, paths=P5}),
    ?assertEqual([0,1,2,3,4,5], lists:sort(Result3)),
    Result4 = expand_links([7], 10, #state{nleaves=6, paths=P5}),
    ?assertEqual([0,1,2], lists:sort(Result4)).

intersect_test() ->
    ?assertEqual(true, intersect([1,4,5], [1,2,3])),
    ?assertEqual(true, intersect([4,1,5], [1,2,3])),
    ?assertEqual(true, intersect([4,6,3], [1,2,3])),
    ?assertEqual(false, intersect([4,6,7], [1,2,3])),
    ?assertEqual(false, intersect([], [])),
    ?assertEqual(false, intersect([1], [2,3])),
    ?assertEqual(true, intersect([1], [2,1])).

contains_test() ->
    ?assertEqual(true, contains(1, [1,2,3])),
    ?assertEqual(false, contains(0, [1,2,3])),
    ?assertEqual(false, contains(0, [])).

replication_groups_from_file_test() ->
    {ok, GroupsFile} = file:open(?GROUPSFILE_TEST, [read]),
    RGroups = replication_groups_from_file(GroupsFile, dict:new()),
    file:close(GroupsFile),
    ?assertEqual(3,length(dict:fetch_keys(RGroups))),
    ?assertEqual([1,2],dict:fetch(0, RGroups)),
    ?assertEqual([2,3],dict:fetch(1, RGroups)),
    ?assertEqual([3,4],dict:fetch(2, RGroups)).

tree_from_file_test() ->
    {ok, TreeFile} = file:open(?TREEFILE_TEST, [read]),
    case file:read_line(TreeFile) of
        eof ->
            eof;
        {error, _Reason} ->
            error;
        {ok, Line} ->
            {NLeaves, []} = string:to_integer(hd(string:tokens(Line, "\n"))),
            {Tree, Paths} = tree_from_file(TreeFile, 0, NLeaves, dict:new(), dict:new()),
            ?assertEqual(3, NLeaves),

            ?assertEqual(2,length(dict:fetch_keys(Paths))),
            ?assertEqual([0,1,4],dict:fetch(3, Paths)),
            ?assertEqual([2,3],dict:fetch(4, Paths)),

            ?assertEqual(5,length(dict:fetch_keys(Tree))),
            ?assertEqual([-1,1,2,3,-1],dict:fetch(0, Tree)),
            ?assertEqual([4,-1,5,6,-1],dict:fetch(1, Tree)),
            ?assertEqual([7,8,-1,-1,9],dict:fetch(2, Tree)),
            ?assertEqual([10,11,-1,-1,12],dict:fetch(3, Tree)),
            ?assertEqual([-1,-1,13,14,-1],dict:fetch(4, Tree))
    end,
    file:close(TreeFile).

is_leaf_test() ->
    ?assertEqual(true, is_leaf(3, 4)),
    ?assertEqual(false, is_leaf(4, 4)).

find_internal_test() ->
    List = [100, 200, 300, 100, -1, -1, 90, -1],
    NLeaves = 4,
    Index = find_internal(List, 0, NLeaves),
    ?assertEqual(6, Index).

-endif.
                    
