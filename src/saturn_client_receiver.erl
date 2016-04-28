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
-module(saturn_client_receiver).
-behaviour(gen_server).

-include("saturn_leaf.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).
-export([handle/3]).

-record(state, {myid}).
               
reg_name(MyId) -> list_to_atom(integer_to_list(MyId) ++ node() ++ atom_to_list(?MODULE)). 

start_link(MyId) ->
    gen_server:start({global, reg_name(MyId)}, ?MODULE, [MyId], []).

handle(MyId, read, [BKey, Clock]) ->
    gen_server:call({global, reg_name(MyId)}, {read, BKey, Clock}, infinity);

handle(MyId, update, [BKey, Value, Clock]) ->
    gen_server:call({global, reg_name(MyId)}, {update, BKey, Value, Clock}, infinity).

init([MyId]) ->
    {ok, #state{myid=MyId}}.

handle_call({read, BKey, Clock}, From, S0) ->
    saturn_leaf:async_read(BKey, Clock, From),
    {noreply, S0};

handle_call({update, BKey, Value, Clock}, From, S0) ->
    saturn_leaf:async_read(BKey, Value, Clock, From),
    {noreply, S0}.

handle_cast(_Info, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

-ifdef(TEST).

-endif.
