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
-module(simple_overlapping_ets_backend_connector).

-include("saturn_leaf.hrl").

-export([update/2,
         read/2,
         connect/1,
         clean/2
        ]).

update(ETS, Payload)->
    {BKey, Value, TimeStamp} = Payload,
    true =  ets:insert(ETS, {BKey, {Value, TimeStamp}}),
    {ok, ETS}.

read(ETS, Payload)->
    {BKey} = Payload,
    case ets:lookup(ETS, BKey) of
        [] ->
            {ok, {empty, dict:new()}};
        [{BKey, Value}] ->
            {ok, Value}
    end.

connect([Partition]) ->
    Name = integer_to_list(Partition) ++ "kv",
    ets:new(list_to_atom(Name), [set, named_table]).

clean(ETS, Partition) ->
    true = ets:delete(ETS),
    Name = integer_to_list(Partition) ++ "kv",
    ets:new(list_to_atom(Name), [set, named_table, private]),
    ETS.
