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
-module(practi_vv).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([init_max_vclock/1,
         min_between_vclocks/2,
         min_vclock/1,
         max_vclock/1
        ]).

init_max_vclock(Entries) ->
    lists:foldl(fun(Entry, Vector) ->
                dict:store(Entry, infinity, Vector)
               end, dict:new(), Entries).

min_between_vclocks(NewV, BaseV) ->
    Entries = dict:fetch_keys(NewV),
    lists:foldl(fun(Entry, Vector) ->
                    ClockNew = dict:fetch(Entry, NewV),
                    ClockBase = dict:fetch(Entry, Vector),
                    case ClockNew < ClockBase of
                        true ->
                            dict:store(Entry, ClockNew, Vector);
                        false ->
                            Vector
                    end
                end, BaseV, Entries).

min_vclock(Vector) ->
   lists:foldl(fun({_Id, Entry}, Min) ->
                min(Entry, Min)
               end, infinity, dict:to_list(Vector)).

max_vclock(Vector) ->
   lists:foldl(fun({_Id, Entry}, Max) ->
                max(Entry, Max)
               end, 0, dict:to_list(Vector)).

-ifdef(TEST).
init_max_vclock_test() ->
    Entries = [e1, e2, e3],
    Vector = init_max_vclock(Entries),
    ?assertEqual(infinity, dict:fetch(e1, Vector)),
    ?assertEqual(infinity, dict:fetch(e2, Vector)),
    ?assertEqual(infinity, dict:fetch(e3, Vector)).

min_between_vclocks_test() ->
    D0 = dict:store(entry1, 6, dict:new()),
    D1 = dict:store(entry2, 7, D0),
    D2 = dict:store(entry3, 0, D1),

    C0 = dict:store(entry1, 5, dict:new()),
    C1 = dict:store(entry2, 9, C0),
    C2 = dict:store(entry3, 1, C1),

    Vector = min_between_vclocks(D2, C2),

    ?assertEqual(5, dict:fetch(entry1, Vector)),
    ?assertEqual(7, dict:fetch(entry2, Vector)),
    ?assertEqual(0, dict:fetch(entry3, Vector)).
    
min_vclock_test() ->
    D0 = dict:store(entry1, 6, dict:new()),
    D1 = dict:store(entry2, 7, D0),
    D2 = dict:store(entry3, 0, D1),
    ?assertEqual(0, min_vclock(D2)).

max_vclock_test() ->
    D0 = dict:store(entry1, 6, dict:new()),
    D1 = dict:store(entry2, 7, D0),
    D2 = dict:store(entry3, 0, D1),
    ?assertEqual(7, max_vclock(D2)).

-endif.
