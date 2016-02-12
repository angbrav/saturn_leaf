%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015-2016 INESC-ID, Instituto Superior Tecnico,
%%                         Universidade de Lisboa, Portugal
%% Copyright (c) 2015-2016 Universite Catholique de Louvain, Belgium
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(riak_connector).

-include("saturn_leaf.hrl").
-include("riak_backend_test.hrl").

-export([update/2,
         read/2,
         propagation/2,
         connect/1
        ]).

update(Riak, Payload)->
    {BKey, Value, TimeStamp} = Payload,
    {Bucket, Key} = BKey,
    BucketB = term_to_binary(Bucket),
    KeyB = term_to_binary(Key),
    Obj = riakc_obj:new(BucketB, KeyB, {Value, TimeStamp}),
    write_to_riak(Riak, Obj).

read(Riak, Payload)->
    {BKey} = Payload,
    {Bucket, Key} = BKey,
    BucketB = term_to_binary(Bucket),
    KeyB = term_to_binary(Key),
    case riakc_pb_socket:get(Riak, BucketB, KeyB) of
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
    {BKey, Value, TimeStamp} = Payload,
    {Bucket, Key} = BKey,
    BucketB = term_to_binary(Bucket),
    KeyB = term_to_binary(Key),
    case riakc_pb_socket:get(Riak, BucketB, KeyB) of
        {error, notfound} ->
            Obj = riakc_obj:new(BucketB, KeyB, {Value, TimeStamp}),
            write_to_riak(Riak, Obj);
        {ok, ObjR} ->
            {_OldValue, OldTimeStamp} = binary_to_term(riakc_obj:get_value(ObjR)),
            case OldTimeStamp >= TimeStamp of
                true ->
                    {ok, Riak};
                false ->
                    Obj2 = riakc_obj:update_value(ObjR, {Value, TimeStamp}),
                    write_to_riak(Riak, Obj2)
            end;
        Else -> 
            lager:error("Unexpected result when reading from Riak: ~p", [Else]),
            {error, Else}
    end.

connect(_) ->
    Port = app_helper:get_env(saturn_leaf, riak_port),
    Node = app_helper:get_env(saturn_leaf, riak_node),
    %lager:info("Connecting to riak at ~p:~p", [Node, Port]),
    {ok, Pid} = riakc_pb_socket:start_link(Node, Port),
    Pid.

write_to_riak(Riak, Obj) ->
    case riakc_pb_socket:put(Riak, Obj) of
        ok ->
            {ok, Riak};
        Else ->
            lager:error("Unexpected result when writing to Riak: ~p", [Else]),
            {error, Else}
    end.

