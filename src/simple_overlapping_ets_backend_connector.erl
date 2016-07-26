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
         remote_read/2,
         init_update/2,
         connect/1,
         clean/2
        ]).

update(ETS, Payload) ->
    {BKey, Value, TimeStamp, Origin} = Payload,
    case ets:lookup(ETS, BKey) of
        [] ->
            true =  ets:insert(ETS, {BKey, {1, [{{TimeStamp, Origin}, Value}]}});
        [{BKey, {Length,[{TSMax, _}=First|Rest]=List}}] ->
            case (TSMax<{TimeStamp, Origin}) of
                true ->
                    List1 = [{{TimeStamp, Origin}, Value}|List];
                false ->
                    List1 = find_position(Rest, {TimeStamp, Origin}, Value, [First])
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
    true =  ets:insert(ETS, {BKey, {1, [{{0, init}, bottom}]}}),
    {ok, ETS}.

read(ETS, Payload)->
    {BKey, Version, MyId} = Payload,
    case ets:lookup(ETS, BKey) of
        [] ->
            {ok, {empty, 0}};
        [{BKey, {_Length,[{TSMax, ValueMax}|_Rest]=List}}] ->
            case Version of
                latest ->
                    {TimeStamp, _Origin} = TSMax,
                    {ok, {ValueMax, TimeStamp}};
                _ ->
                    get_version(List, Version, MyId)
            end
    end.

get_version([], _Version, _MyId) ->
    {ok, {empty, 0}};

get_version([Next|Rest], Version, MyId) ->
    %lager:info("Next is ~p", [Next]),
    {{TimeStamp, Origin}, Value} = Next,
    case Origin of 
        MyId ->
            {ok, {Value, TimeStamp}};
        _ ->
            case (TimeStamp > Version) of
                true ->
                    get_version(Rest, Version, MyId);
                false ->
                    {ok, {Value, TimeStamp}}
            end
    end.

remote_read(ETS, Payload)->
    {BKey, Version} = Payload,
    case ets:lookup(ETS, BKey) of
        [] ->
            {ok, {empty, 0}};
        [{BKey, {_Length,[{TSMax, ValueMax}|_Rest]=List}}] ->
            case Version of
                latest ->
                    {TimeStamp, _Origin} = TSMax,
                    {ok, {ValueMax, TimeStamp}};
                _ ->
                    get_version_remote(List, Version)
            end
    end.

get_version_remote([], _Version) ->
    {ok, {empty, 0}};

get_version_remote([Next|Rest], Version) ->
    %lager:info("Next is ~p", [Next]),
    {{TimeStamp, _Origin}, Value} = Next,
    case (TimeStamp > Version) of
        true ->
            get_version_remote(Rest, Version);
        false ->
            {ok, {Value, TimeStamp}}
    end.

connect([Partition]) ->
    Name = integer_to_list(Partition) ++ "kv",
    ets:new(list_to_atom(Name), [set, named_table, private]).

clean(ETS, Partition) ->
    true = ets:delete(ETS),
    Name = integer_to_list(Partition) ++ "kv",
    ets:new(list_to_atom(Name), [set, named_table, private]),
    ETS.

find_position([], Version, Value, Head) ->
    List = [{Version,Value}|Head],
    lists:reverse(List);

find_position([Next|Rest]=List, Version, Value, Head) ->
    {TS, _} = Next,
    case (Version > TS) of
        true ->
            List1 = [{Version, Value}|List],
            lists:foldl(fun(Elem, Acc) ->
                            [Elem|Acc]
                        end, List1, Head);
        false ->
            find_position(Rest, Version, Value, [Next|Head])
    end.
