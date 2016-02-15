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
-module(saturn_utilities).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([binary_search/3,
         now_milisec/0,
         now_microsec/0]).

binary_search(List, N, Comparator) ->
    binary_search(List, N, Comparator, 0).

binary_search(List, N, Comparator, Carrying) ->
    Length = length(List),
    Middle = (Length + 1) div 2,
    case Middle of
        0 ->
            {error, not_found};
        _ ->
            Item = lists:nth(Middle, List),
            case Comparator(N, Item) of
                equal ->
                    {ok, Carrying + Middle}; 
                lesser ->
                    binary_search(lists:sublist(List, Length - Middle), N, Comparator, Carrying); %% LT, search on left side
                _ ->
                    binary_search(lists:nthtail(Middle, List), N, Comparator, Carrying + Middle)           %% GT, search on right side
            end
    end.

now_microsec()->
    %% Not very efficient. os:timestamp() faster but non monotonic. Test!
    {MegaSecs, Secs, MicroSecs} = os:timestamp(),
    (MegaSecs * 1000000 + Secs) * 1000000 + MicroSecs.

now_milisec() ->
    now_microsec()/1000.

-ifdef(TEST).

binary_search_test() ->
    Comparator1 = fun(Item1, Item2) ->
                    {Clock1, _} = Item1,
                    {Clock2, _} = Item2,
                    case Clock1>Clock2 of
                        true ->
                            greater;
                        false ->
                            case Clock1==Clock2 of
                                true ->
                                    equal;
                                false ->
                                    lesser
                            end
                    end
                  end,
    List = [{1,dc1},{2,dc1},{3,dc1},{4,dc1},{5,dc1}],
    Index1 = binary_search(List, {1,dc1}, Comparator1),
    ?assertEqual({ok, 1}, Index1),
    Index2 = binary_search(List, {2,dc1}, Comparator1),
    ?assertEqual({ok, 2}, Index2),
    Index3 = binary_search(List, {3,dc1}, Comparator1),
    ?assertEqual({ok, 3}, Index3),
    Index4 = binary_search(List, {4,dc1}, Comparator1),
    ?assertEqual({ok, 4}, Index4),
    Index5 = binary_search(List, {5,dc1}, Comparator1),
    ?assertEqual({ok, 5}, Index5),
    Index6 = binary_search(List, {6,dc1}, Comparator1),
    ?assertEqual({error, not_found}, Index6),
    Index7 = binary_search([], {6,dc1}, Comparator1),
    ?assertEqual({error, not_found}, Index7).

-endif.
