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
-module(saturn_leaf_converger).
-behaviour(gen_server).

-include("saturn_leaf.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).
-export([handle/2]).

-record(state, {pending_id}).
               

start_link() ->
    Name = list_to_atom(atom_to_list(node()) ++ atom_to_list(?MODULE)),
    gen_server:start({global, Name}, ?MODULE, [], []).

handle(Name, Message) ->
    %lager:info("Message received: ~p", [Message]),
    gen_server:cast({global, Name}, Message).

init([]) ->
    {ok, #state{pending_id=0}}.

handle_cast({new_operation, Label, Value}, S0=#state{pending_id=PId0}) ->
    %lager:info("New operation received. Label: ~p", [Label]),
    DocIdx = riak_core_util:chash_key({?BUCKET_COPS, PId0}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?COPS_SERVICE),
    [{IndexNode, _Type}] = PrefList,
    saturn_cops_vnode:update(IndexNode, PId0, Label, Value),
    PId1 = PId0 + 1,
    {noreply, S0#state{pending_id=PId1}};

handle_cast({remote_update, Label, Value}, S0=#state{pending_id=PId0}) ->
    %lager:info("New operation received. Label: ~p", [Label]),
    DocIdx = riak_core_util:chash_key({?BUCKET_COPS, PId0}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?COPS_SERVICE),
    [{IndexNode, _Type}] = PrefList,
    saturn_cops_vnode:remote_update(IndexNode, PId0, Label, Value),
    PId1 = PId0 + 1,
    {noreply, S0#state{pending_id=PId1}};

handle_cast({remote_read, Label}, S0=#state{pending_id=PId0}) ->
    %lager:info("New operation received. Label: ~p", [Label]),
    DocIdx = riak_core_util:chash_key({?BUCKET_COPS, PId0}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?COPS_SERVICE),
    [{IndexNode, _Type}] = PrefList,
    saturn_cops_vnode:remote_read(IndexNode, PId0, Label),
    PId1 = PId0 + 1,
    {noreply, S0#state{pending_id=PId1}};

handle_cast({remote_reply, Label}, S0=#state{pending_id=PId0}) ->
    %lager:info("New operation received. Label: ~p", [Label]),
    DocIdx = riak_core_util:chash_key({?BUCKET_COPS, PId0}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?COPS_SERVICE),
    [{IndexNode, _Type}] = PrefList,
    saturn_cops_vnode:remote_reply(IndexNode, PId0, Label),
    PId1 = PId0 + 1,
    {noreply, S0#state{pending_id=PId1}};

handle_cast(_Info, State) ->
    {noreply, State}.

handle_call(_Info, _From, State) ->
    {reply, error, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

-ifdef(TEST).

-endif.
