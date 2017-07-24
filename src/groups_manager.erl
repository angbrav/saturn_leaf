-module(groups_manager).

-include("saturn_leaf.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([get_datanodes_ids/3,
         filter_stream_leaf_id/6,
         new_treefile/1,
         new_groupsfile/2,
         interested/6,
         get_mypath/2,
         get_bucket_sample/2,
         is_leaf/2,
         get_closest_dcid/4,
         get_all_nodes/1,
         get_all_nodes_but_myself/2,
         get_delay_leaf/3,
         get_delays_internal/2,
         init_state/1,
         path_from_tree_dict/2,
         set_groups/2,
         on_path/5,
         do_replicate/3]).

init_state(Id) ->
    Name = Id ++ "groups",
    Groups = ets:new(list_to_atom(Name), [set, named_table, private]),
    {ok, GroupsFile} = application:get_env(saturn_leaf, groups),
    {ok, TreeFile} = application:get_env(saturn_leaf, tree),
    ok = groups_manager:new_groupsfile(GroupsFile, Groups),
    {ok, Paths, Tree, NLeaves} = groups_manager:new_treefile(TreeFile),
    #state_manager{tree=Tree,
                   paths=Paths,
                   nleaves=NLeaves,
                   groups=Groups}.

set_groups(Table, Groups) ->
    true = ets:delete_all_objects(Table),
    lists:foreach(fun(Item) ->
                    true = ets:insert(Table, Item)
                  end, dict:to_list(Groups)),
    ok.

get_all_nodes_but_myself(Tree, MyId) ->
    Nodes = dict:fetch_keys(Tree),
    {ok, lists:delete(MyId, Nodes)}.

get_all_nodes(Tree) ->
    Nodes = dict:fetch_keys(Tree),
    {ok, Nodes}.

get_mypath(MyId, Paths) ->
    case dict:find(MyId, Paths) of
        {ok, Value} ->
            {ok, Value};
        error ->
            lager:error("Wrong deafult tree file, returning []"),
            {ok, []}
    end.

get_bucket_sample(MyId, Groups) ->
    case find_key(ets:first(Groups), Groups, MyId) of
        {ok, Bucket} -> 
            {ok, Bucket};
        {error, not_found} ->
            {error, not_found}
    end.
    
get_delay_leaf(Tree, MyId, NLeaves) ->
    Row = dict:fetch(MyId, Tree),
    Internal = find_internal(Row, 0, NLeaves),
    Delay = lists:nth(Internal+1, Row),
    {ok, Delay}.

get_delays_internal(Tree, MyId) ->
    Row = dict:fetch(MyId, Tree),
    {Delays, _} = lists:foldl(fun(Entry, {Dict, C}) ->
                                case Entry >= 0 of
                                    true ->
                                        {dict:store(C, Entry, Dict), C+1};
                                    false ->
                                        {Dict, C+1}
                                end
                              end, {dict:new(), 0}, Row),
    {ok, Delays}.
    %case dict:size(Delays) > 3 of
    %    true ->
    %        lager:error("Tree is not binary: ~p: ~p", [MyId, Row]),
    %        {error, no_binary_tree};
    %    false ->
    %        {ok, Delays}
    %end.

new_groupsfile(File, Table) ->
    {ok, GroupsFile} = file:open(File, [read]),
    true = ets:delete_all_objects(Table),
    ok = replication_groups_from_file(GroupsFile, Table),
    file:close(GroupsFile),
    ok.

new_treefile(File) ->
    {ok, TreeFile} = file:open(File, [read]),
    case file:read_line(TreeFile) of
        eof ->
            lager:error("Empty file: ~p", [File]),
            file:close(TreeFile),
            {error, empty_file};
        {error, Reason} ->
            lager:error("Problem reading ~p file, reason: ~p", [File, Reason]),
            file:close(TreeFile),
            {error, Reason};
        {ok, Line} ->
            {NLeaves, []} = string:to_integer(hd(string:tokens(Line, "\n"))),
            {Tree, Paths} = tree_from_file(TreeFile, 0, NLeaves, dict:new(), dict:new()),
            file:close(TreeFile),
            {ok, Paths, Tree, NLeaves}
    end.

do_replicate(BKey, Groups, MyId) ->
    {Bucket, _Key} = BKey,
    case ets:lookup(Groups, Bucket) of 
        [] ->
            {error, unknown_key};
        [{Bucket, Value}]->
            case contains(MyId, Value) of
                true ->
                    true;
                false ->
                    %lager:info("Node: ~p does not replicate: ~p (Replicas: ~p)", [MyId, BKey, Value]),
                    false
            end
    end.
        
get_closest_dcid(BKey, Groups, MyId, Tree) ->
    {Bucket, _Key} = BKey,
    case ets:lookup(Groups, Bucket) of
        [] ->
            {error, unknown_key};
        [{Bucket, Value}] ->
            {ClosestId, _} = lists:foldl(fun(Id, {_IdClosest, D0}=Acc) ->
                                            D1 = distance_datanodes(Tree, MyId, Id),
                                            case D0 of
                                                max -> {Id, D1};
                                                _ -> case (D1 > D0) of
                                                        true -> Acc;
                                                        false -> {Id, D1}
                                                     end
                                            end
                                         end, {noid, max}, Value),
            case ClosestId of
                noid ->
                    {error, no_replica_found};
                _ ->
                    {ok, ClosestId}
            end
    end.


get_datanodes_ids(BKey, Groups, MyId) ->
    {Bucket, _Key} = BKey,
    case ets:lookup(Groups, Bucket) of
        [] ->
            {error, unknown_key};
        [{Bucket, Value}] ->
            Group = lists:foldl(fun(Id, Acc) ->
                                    case Id of
                                        MyId ->
                                            Acc;
                                        _ ->
                                            Acc ++ [Id]
                                    end
                                end, [], Value),
            {ok, Group}
    end.

filter_stream_leaf_id(Stream0, Tree, NLeaves, MyId, Groups, Paths) ->
    Row = dict:fetch(MyId, Tree),
    Internal = find_internal(Row, 0, NLeaves),
    Stream1 = lists:foldl(fun({BKey, Elem}, Acc) ->
                            {Bucket, _Key} = BKey,
                            case Elem#label.operation of
                                update ->
                                    case interested(Internal, Bucket, MyId, Groups, NLeaves, Paths) of
                                        true ->
                                            Acc ++ [Elem];
                                        false ->
                                            Acc
                                    end;
                                _ ->
                                    Acc ++ [Elem]
                                end
                          end, [], Stream0),
    {ok, Stream1, Internal}.

on_path(Id, Id, _PreId, _Paths, _NLeaves) ->
    true;

on_path(Id, Destination, PreId, Paths, NLeaves) ->
    case is_leaf(Id, NLeaves) of
        true ->
            false;
        false ->
            Links = dict:fetch(Id, Paths),
            FilteredLinks = lists:foldl(fun(Elem, Acc) ->
                                        case Elem of
                                            PreId -> Acc;
                                            _ -> Acc ++ [Elem]
                                        end
                                       end, [], Links),
            LinksExpanded = expand_links(FilteredLinks, Id, NLeaves, Paths),
            contains(Destination, LinksExpanded)
    end.

interested(Id, Bucket, PreId, Groups, NLeaves, Paths) ->
    [{Bucket, Group}] = ets:lookup(Groups, Bucket),
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
            LinksExpanded = expand_links(FilteredLinks, Id, NLeaves, Paths),
            intersect(LinksExpanded, Group)
    end.

is_leaf(Id, Total) ->
    Id<Total.

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

%%private
            
expand_links([], _PreId, _NLeaves, _Paths) ->
    [];
               
expand_links([H|T], PreId, NLeaves, Paths) ->
    case is_leaf(H, NLeaves) of
        true ->
            [H] ++ expand_links(T, PreId, NLeaves, Paths);
        false ->
            ExtraPath = dict:fetch(H, Paths),
            FilteredExtraPath = lists:foldl(fun(Elem, Acc) ->
                                            case Elem of
                                                PreId -> Acc;
                                                _ -> Acc ++ [Elem]
                                            end 
                                           end, [], ExtraPath),
            expand_links(FilteredExtraPath, H, NLeaves, Paths) ++ expand_links(T, PreId, NLeaves, Paths)
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

replication_groups_from_file(Device, Table)->
    case file:read_line(Device) of
        eof ->
            ok;
        {error, Reason} ->
            lager:error("Problem reading ~p file, reason: ~p", [?GROUPSFILE, Reason]),
            {error, Reason};
        {ok, "\n"} ->
            replication_groups_from_file(Device, Table);
        {ok, Line} ->
            [H|T] = string:tokens(hd(string:tokens(Line,"\n")), ","),
            ReplicationGroup = lists:foldl(fun(Elem, Acc) ->
                                            {Int, []} = string:to_integer(Elem),
                                            Acc ++ [Int]
                                           end, [], T),
            {Key, []} = string:to_integer(H),
            true = ets:insert(Table, {Key, ReplicationGroup}),
            replication_groups_from_file(Device, Table)
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

find_key(Key, Groups, MyId) ->
    case Key of
        '$end_of_table' ->
            {error, not_found};
        _ ->
            [{Key, Ids}] = ets:lookup(Groups, Key),
            case lists:member(MyId, Ids) of
                true ->
                    {ok, Key};
                false ->
                    find_key(ets:next(Groups, Key), Groups, MyId)
            end
    end.

distance_datanodes(Tree, MyId, Id) ->
    Row = dict:fetch(MyId, Tree),
    lists:nth(Id+1, Row).

-ifdef(TEST).
distance_datanodes_test() ->
    P1 = dict:store(0,[-1,1,2,3,-1],dict:new()),
    P2 = dict:store(1,[4,-1,5,6,-1],P1),
    P3 = dict:store(2,[7,8,-1,-1,9],P2),
    P4 = dict:store(3,[10,11,-1,-1,12],P3),
    P5 = dict:store(4,[-1,-1,13,14,-1],P4),
    D = distance_datanodes(P5, 1, 2),
    ?assertEqual(5, D).
    
interested_test() ->
    P1 = dict:store(6,[0,1,7],dict:new()),
    P2 = dict:store(7,[2,6,10],P1),
    P3 = dict:store(8,[3,9,10],P2),
    P4 = dict:store(9,[4,5,8],P3),
    P5 = dict:store(10,[7,8],P4),
    Groups = ets:new(test, [set, named_table]),
    true = ets:insert(Groups, {3, [0,1,3]}),
    ?assertEqual(true, interested(0, 3, 6, Groups, 6, P5)),
    ?assertEqual(false, interested(2, 3, 7, Groups, 6, P5)),
    ?assertEqual(true, interested(7, 3, 10, Groups, 6, P5)),
    ?assertEqual(false, interested(9, 3, 8, Groups, 6, P5)),
    true = ets:delete(Groups).

expand_links_test() ->
    P1 = dict:store(6,[0,1,7],dict:new()),
    P2 = dict:store(7,[2,6,10],P1),
    P3 = dict:store(8,[3,9,10],P2),
    P4 = dict:store(9,[4,5,8],P3),
    P5 = dict:store(10,[7,8],P4),
    Result1 = expand_links([6], 0, 6, P5),
    ?assertEqual([1,2,3,4,5], lists:sort(Result1)),
    Result2 = expand_links([7], 2, 6,P5),
    ?assertEqual([0,1,3,4,5], lists:sort(Result2)),
    Result3 = expand_links([7,8], 10, 6, P5),
    ?assertEqual([0,1,2,3,4,5], lists:sort(Result3)),
    Result4 = expand_links([7], 10, 6, P5),
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
    Test = ets:new(test, [set, named_table]),
    ok = replication_groups_from_file(GroupsFile, Test),
    file:close(GroupsFile),
    ?assertEqual(3,ets:info(Test, size)),
    ?assertEqual([{0,[1,2]}],ets:lookup(Test, 0)),
    ?assertEqual([{1,[2,3]}],ets:lookup(Test, 1)),
    ?assertEqual([{2,[3,4]}],ets:lookup(Test, 2)),
    true = ets:delete(Test).

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
