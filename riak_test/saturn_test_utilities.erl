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
-module(saturn_test_utilities).

-export([eventual_read/3, 
         eventual_read/4,
         eventual_da_read/3, 
         eventual_da_read/4,
         stop_datastore/1,
         server_name/1,
         clean_datastore_data/1]).

-include("saturn_leaf.hrl").
-include("riak_backend_test.hrl").

server_name(Node)->
    {saturn_client_receiver, Node}.
    %{global, list_to_atom(atom_to_list(Node) ++ atom_to_list(saturn_client_receiver))}.

eventual_read(Key, Node, ExpectedResult) ->
    eventual_read(Key, Node, ExpectedResult, 0).

eventual_read(Key, Node, ExpectedResult, Clock) ->
    Result=rpc:call(Node, saturn_leaf, read, [Key, Clock]),
    case Result of
        {ok, {ExpectedResult, _Clock}} -> Result;
        _ ->
            lager:info("I read: ~p, expecting: ~p",[Result, ExpectedResult]),
            timer:sleep(500),
            eventual_read(Key, Node, ExpectedResult)
    end.

eventual_da_read(Key, Node, ExpectedResult) ->
    eventual_da_read(Key, Node, ExpectedResult, 0).

eventual_da_read(Key, Node, ExpectedResult, Clock) ->
    Result=gen_server:call(server_name(Node), {read, Key, Clock}, infinity),
    %Result=rpc:call(Node, saturn_leaf, read, [Key, Clock]),
    case Result of
        {ok, {ExpectedResult, _Clock}} -> Result;
        _ ->
            lager:info("I read: ~p, expecting: ~p",[Result, ExpectedResult]),
            timer:sleep(500),
            eventual_read(Key, Node, ExpectedResult)
    end.

clean_datastore_data([]) ->
    ok;

clean_datastore_data([Id|Rest]) ->
    case ?BACKEND of
        simple_backend ->
            ok; 
        simple_overlapping_backend ->
            ok;
        riak_backend ->
            IdString = integer_to_list(Id),
            DataDir = ?RIAK_PATH_BASE ++ IdString ++ "/data",
            Executable = ?RIAK_PATH_BASE ++ IdString ++ "/bin/riak ",
            lager:info("Stopping riak: ~p", [Executable ++ "stop"]),
            os:cmd(Executable ++ "stop"),
            lager:info("Removing data dir: ~p", ["rm -rf " ++ DataDir]),
            os:cmd("rm -rf " ++ DataDir),
            lager:info("Creating data dir: ~p", ["mkdir " ++ DataDir]),
            os:cmd("mkdir " ++ DataDir),
            lager:info("Starting riak: ~p", [Executable ++ "start"]),
            os:cmd(Executable ++ "start"),
            Node = "riak" ++ IdString ++ "@127.0.0.1",
            rt:wait_for_service(list_to_atom(Node), riak_kv),
            clean_datastore_data(Rest);
        _ ->
            ok
    end.

stop_datastore([]) ->
    ok;

stop_datastore([Id|Rest]) ->
    case ?BACKEND of
        simple_backend ->
            ok;
        riak_backend ->
            IdString = integer_to_list(Id),
            Executable = ?RIAK_PATH_BASE ++ IdString ++ "/bin/riak ",
            lager:info("Stopping riak: ~p", [Executable ++ "stop"]),
            os:cmd(Executable ++ "stop"),
            stop_datastore(Rest);
        _ ->
            ok
    end.
