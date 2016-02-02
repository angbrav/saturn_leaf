-module(riak_connector).

-include("saturn_leaf.hrl").
-include("riak_backend_test.hrl").

-export([update/2,
         read/2,
         propagation/2,
         connect/0
        ]).

update(Riak, Payload)->
    {Key, Value, TimeStamp} = Payload,
    KeyB = term_to_binary(Key),
    Obj = riakc_obj:new(?BUCKET, KeyB, {Value, TimeStamp}),
    write_to_riak(Riak, Obj).

read(Riak, Payload)->
    {Key} = Payload,
    KeyB = term_to_binary(Key),
    case riakc_pb_socket:get(Riak, ?BUCKET, KeyB) of
        {error, notfound} ->
            {ok, {empty, 0}};
        {ok, Obj} ->
            lager:info(riakc_obj:get_value(Obj)),
            {ok, binary_to_term(riakc_obj:get_value(Obj))};
        Else ->
            lager:error("Unexpected result when reading from Riak: ~p", [Else]),
            {error, Else}
    end.

propagation(Riak, Payload)->
    {Key, Value, TimeStamp} = Payload,
    KeyB = term_to_binary(Key),
    case riakc_pb_socket:get(Riak, ?BUCKET, KeyB) of
        {error, notfound} ->
            Obj = riakc_obj:new(?BUCKET, KeyB, {Value, TimeStamp}),
            write_to_riak(Riak, Obj);
        {ok, ObjR} ->
            {_OldValue, OldTimeStamp} = binary_to_term(riakc_obj:get_value(ObjR)),
            case OldTimeStamp >= TimeStamp of
                true ->
                    ok;
                false ->
                    Obj2 = riakc_obj:update_value(ObjR, {Value, TimeStamp}),
                    write_to_riak(Riak, Obj2)
            end;
        Else -> 
            lager:error("Unexpected result when reading from Riak: ~p", [Else]),
            {error, Else}
    end.

connect() ->
    Port = app_helper:get_env(saturn_leaf, riak_port),
    Node = app_helper:get_env(saturn_leaf, riak_node),
    %lager:info("Connecting to riak at ~p:~p", [Node, Port]),
    {ok, Pid} = riakc_pb_socket:start_link(Node, Port),
    Pid.

write_to_riak(Riak, Obj) ->
    case riakc_pb_socket:put(Riak, Obj) of
        ok ->
            ok;
        Else ->
            lager:error("Unexpected result when writing to Riak: ~p", [Else]),
            {error, Else}
    end.

