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

-record(state, {labels_queue :: queue(),
                ops_dict :: dict()}).
               

start_link() ->
    gen_server:start({local, ?MODULE}, ?MODULE, [], []).

handle(Name, Message) ->
    %lager:info("Message received: ~p", [Message]),
    gen_server:cast(Name, Message).

init([]) ->
    {ok, #state{labels_queue=queue:new(),
                ops_dict=dict:new()}}.

handle_cast({new_operation, Label, Value}, S0) ->
    lager:info("New operation received. Label: ~p", [Label]),
    ok = execute_operation(Label, Value),
    {noreply, S0};

handle_cast({remote_read, Label}, S0) ->
    %lager:info("Remote read received. Label: ~p", [Label]),
    BKey = Label#label.bkey,
    DocIdx = riak_core_util:chash_key(BKey),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
    [{IndexNode, _Type}] = PrefList,
    saturn_proxy_vnode:remote_read(IndexNode, Label),
    {noreply, S0};

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

execute_operation(Label, Value) ->
    BKey = Label#label.bkey,
    Clock = Label#label.timestamp,
    Sender = Label#label.sender,
    DocIdx = riak_core_util:chash_key(BKey),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
    [{IndexNode, _Type}] = PrefList,
    saturn_proxy_vnode:propagate(IndexNode, BKey, Value, {Clock, Sender}).

-ifdef(TEST).

-endif.
