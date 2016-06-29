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

-module(read_tx_coord_fsm).

-behavior(gen_fsm).

-include("saturn_leaf.hrl").

%% API
-export([start_link/1]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export([idle/2,
         collect_reads/2,
         reply_client/2]).

-record(state, {
          vnode,
          client,
          clock,
          result,
          total :: integer()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(VNode) ->
    gen_fsm:start_link(?MODULE, [VNode], []).

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state.
init([VNode]) ->
    State = #state{vnode=VNode},
    {ok, idle, State, 0}.

idle({new_tx, BKeys, Client}, S0) ->
    lists:foreach(fun(BKey) ->
                    DocIdx = riak_core_util:chash_key(BKey),
                    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
                    [{IndexNode, _Type}] = PrefList,
                    saturn_proxy_vnode:fsm_read(IndexNode, BKey, self())
                  end, BKeys),
    {next_state, collect_reads, S0#state{total=length(BKeys), client=Client, result=[]}};

idle(_, S0) ->
    {next_state, idle, S0}.

collect_reads({error, Reason}, S0=#state{client=Client}) ->
    gen_server:reply(Client, {error, Reason}),
    {next_state, idle, S0};
    
collect_reads({new_value, BKey, Value}, S0=#state{result=Result0, total=Total}) ->
    case Total of
        1 ->
            {next_state, reply_client, S0#state{result=[{BKey, Value}|Result0], total=0}, 0};
        _ ->
            {next_state, collect_reads, S0#state{result=[{BKey, Value}|Result0], total=Total-1}}
    end.

reply_client(timeout, S0=#state{result=Result, client=Client, vnode=VNode}) ->
    gen_server:reply(Client, {ok, {Result, 0}}),
    saturn_proxy_vnode:fsm_idle(VNode, self()),
    {next_state, idle, S0}.

handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.
