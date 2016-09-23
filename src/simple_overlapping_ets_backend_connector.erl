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
         init_update/2,
         connect/1,
         clean/2
        ]).

update(ETS, Payload) ->
    {BKey, Value, TimeStamp} = Payload,
    VV = lists:sort(dict:to_list(TimeStamp)),
    case ets:lookup(ETS, BKey) of
        [] ->
            true =  ets:insert(ETS, {BKey, {1, [{VV, Value}]}});
        [{BKey, {Length,[{TSMax, _}=First|Rest]=List}}] ->
            case (VV > TSMax) of
                true ->
                    List1 = [{VV, Value}|List];
                false ->
                    List1 = find_position(Rest, VV, Value, [First])
            end,
            case Length of
                ?VERSION_THOLD ->
                    true = ets:insert(ETS, {BKey, {Length, lists:sublist(List1, ?VERSION_THOLD)}});
                _ ->
                    true = ets:insert(ETS, {BKey, {Length+1,List1}})
            end
    end,
    {ok, ETS}.

init_update(ETS, Payload) ->
    BKey = Payload,
    true =  ets:insert(ETS, {BKey, {1, [{[], bottom}]}}),
    {ok, ETS}.

read(ETS, Payload)->
    {BKey, Version} = Payload,
    case ets:lookup(ETS, BKey) of
        [] ->
            %lager:error("BKey ~p was not initialized", [BKey]),
            {ok, {empty, dict:new()}};
        [{BKey, {_Length, [{TSMax, ValueMax}|_Rest]=List}}] ->
            case Version of
                latest ->
                    TSDict = dict:from_list(TSMax),
                    {ok, {ValueMax, TSDict}};
                _ ->
                    VV = lists:sort(dict:to_list(Version)),
                    get_version(List, VV)
            end
    end.

get_version([], _Version) ->
    {ok, {empty, dict:new()}};

get_version([Next|Rest], Version) ->
    %lager:info("Next is ~p", [Next]),
    {TimeStamp, Value} = Next,
    case (TimeStamp > Version) of
        true ->
            get_version(Rest, Version);
        false ->
            TSDict = dict:from_list(TimeStamp),
            {ok, {Value, TSDict}}
    end.

connect([Partition]) ->
    Name = integer_to_list(Partition) ++ "kv",
    ets:new(list_to_atom(Name), [set, named_table]).

clean(ETS, Partition) ->
    true = ets:delete(ETS),
    Name = integer_to_list(Partition) ++ "kv",
    ets:new(list_to_atom(Name), [set, named_table, private]),
    ETS.

find_position([], VV, Value, Head) ->
    List = [{VV,Value}|Head],
    lists:reverse(List);

find_position([Next|Rest]=List, VV, Value, Head) ->
    {TS, _} = Next,
    case (VV > TS) of
        true ->
            List1 = [{VV, Value}|List],
            lists:foldl(fun(Elem, Acc) ->
                            [Elem|Acc]
                        end, List1, Head);
        false ->
            find_position(Rest, VV, Value, [Next|Head])
    end.
