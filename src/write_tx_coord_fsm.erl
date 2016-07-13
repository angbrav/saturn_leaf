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

-module(write_tx_coord_fsm).

-behavior(gen_fsm).

-include("saturn_leaf.hrl").

%% API
-export([start_link/1]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export([idle/2,
         collect_prepare/2,
         reply_client/2]).

-record(state, {
          vnode,
          client,
          clock,
          involved,
          remote_updates,
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

idle({new_tx, BKeyValuePairs, Clock, Client}, S0=#state{vnode=VNode}) ->
    Scattered = lists:foldl(fun({BKey, Value}, Acc) ->
                                DocIdx = riak_core_util:chash_key(BKey),
                                PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
                                [{IndexNode, _Type}] = PrefList,
                                dict:append(IndexNode, {BKey, Value}, Acc)
                            end, dict:new(), BKeyValuePairs),
    lists:foreach(fun({IndexNode, Pairs}) ->
                    saturn_proxy_vnode:prepare(IndexNode, {Clock, VNode}, Pairs, Clock, self())
                  end, dict:to_list(Scattered)),
    {next_state, collect_prepare, S0#state{total=dict:size(Scattered), client=Client, clock=Clock, involved=[], remote_updates=[]}, 10000};

idle(_, S0) ->
    {next_state, idle, S0}.

collect_prepare({error, Reason}, S0=#state{client=Client, vnode=VNode}) ->
    gen_server:reply(Client, {error, Reason}),
    saturn_proxy_vnode:write_fsm_idle(VNode, self()),
    {next_state, idle, S0};
    
collect_prepare({prepared, {Ignored, Remote}, IndexNode}, S0=#state{total=Total,
                                                                    vnode=VNode,
                                                                    clock=Clock,
                                                                    involved=Involved0,
                                                                    remote_updates=Pairs0}) ->
    case Ignored of
        false ->
            Involved = [IndexNode|Involved0];
        true ->
            Involved = Involved0
    end,
    Pairs = [{IndexNode, Remote}|Pairs0],
    case Total of
        1 ->
            lists:foreach(fun({P, List}) ->
                            saturn_proxy_vnode:propagate_remote(P, {Clock, VNode}, List)
                          end, Pairs),
            lists:foreach(fun(Elem) ->
                            saturn_proxy_vnode:commit(Elem, {Clock, VNode}, false)
                          end, Involved),
            {next_state, reply_client, S0#state{total=0}, 0};
        _ ->
            {next_state, collect_prepare, S0#state{total=Total-1, involved=Involved, remote_updates=Pairs}}
    end;

collect_prepare(timeout, S0=#state{client=Client, vnode=VNode}) ->
    gen_server:reply(Client, {error, timeout}),
    saturn_proxy_vnode:write_fsm_idle(VNode, self()),
    {next_state, idle, S0}.

reply_client(timeout, S0=#state{client=Client, clock=Clock, vnode=VNode}) ->
    gen_server:reply(Client, {ok, Clock}),
    saturn_proxy_vnode:write_fsm_idle(VNode, self()),
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