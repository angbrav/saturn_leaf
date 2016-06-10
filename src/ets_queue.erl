-module(ets_queue).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([new/1,
         clean/1,
         delete/1,
         in/2,
         out/1,
         is_empty/1,
         peek/1]).

new(Name) ->
    Table = ets:new(Name, [set, named_table, private]),
    {0, 0, Table}.

clean({_, _, Table}) ->
    true = ets:delete_all_objects(Table),
    {0, 0, Table}.

delete({_, _, Table}) ->
    true = ets:delete(Table),
    ok.

in(Element, Queue) ->
    {Head, Tail, Table} = Queue,
    ets:insert(Table, {Tail, Element}),
    {Head, Tail+1, Table}.

out({0, 0, _Table}=Queue) ->
    {empty, Queue};

out({Head, Tail, Table}=Queue) ->
    case ets:lookup(Table, Head) of
        [] ->
            {{error, no_head}, Queue};
        [{Head, Element}] ->
            true = ets:delete(Table, Head),
            case (Head+1==Tail) of
                true ->
                    {{value, Element}, {0, 0, Table}};
                false ->
                    {{value, Element}, {Head+1, Tail, Table}}
            end
    end.

peek({0, 0, _Table}) ->
    empty;

peek({Head, _Tail, Table}) ->
    case ets:lookup(Table, Head) of
        [] ->
            {error, no_head};
        [{Head, Element}] ->
            {value, Element}
    end.

is_empty({0, 0, _Table}) ->
    true;

is_empty({_Head, _Tail, _Table}) ->
    false.

-ifdef(TEST).
ets_queue_test() ->
    Queue0 = new(test_table),
    {Result1, Queue1} = out(Queue0),
    ?assertEqual(empty, Result1),
    Queue2 = in(element1, Queue1),
    {Result2, Queue3} = out(Queue2),
    ?assertEqual({value, element1}, Result2),
    {Result3, Queue4} = out(Queue3),
    ?assertEqual(empty, Result3),
    Queue5 = in(element2, Queue4),
    Queue6 = in(element3, Queue5),
    {Result4, Queue7} = out(Queue6),
    ?assertEqual({value, element2}, Result4),
    {Result5, Queue8} = out(Queue7),
    ?assertEqual({value, element3}, Result5),
    {Head, Tail, _} = Queue8,
    ?assertEqual({0, 0}, {Head, Tail}).
    
-endif.
