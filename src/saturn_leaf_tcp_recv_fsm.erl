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
-module(saturn_leaf_tcp_recv_fsm).
-behaviour(gen_fsm).

-record(state, {port, handler, listener, myid}). % the current socket

-export([start_link/3]).
-export([init/1,
         code_change/4,
         handle_event/3,
         handle_info/3,
         handle_sync_event/4,
         terminate/3]).
-export([accept/2
        ]).

-define(TIMEOUT,10000).

start_link(Port, Handler, MyId) ->
    gen_fsm:start_link(?MODULE, [Port, Handler, MyId], []).

init([Port, Handler, MyId]) ->
    {ok, ListenSocket} = gen_tcp:listen(
                           Port,
                           [{active,false}, binary,
                            {packet,4},{reuseaddr, true}
                           ]),
    {ok, accept, #state{port=Port, listener=ListenSocket, handler=Handler, myid=MyId},0}.

%% Accepts an incoming tcp connection and spawn and new fsm 
%% to process the connection, so that new connections could 
%% be processed in parallel
accept(timeout, State=#state{listener=ListenSocket, handler=Handler, myid=MyId}) ->
    {ok, AcceptSocket} = gen_tcp:accept(ListenSocket),
    {ok, _} = saturn_leaf_tcp_connection_handler_fsm_sup:start_fsm([AcceptSocket, Handler, MyId]),
    {next_state, accept, State, 0}.

handle_info(Message, _StateName, StateData) ->
    lager:error("Recevied info:  ~p",[Message]),
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD=#state{listener=ListenSocket}) ->
    gen_tcp:close(ListenSocket),
    lager:info("Closing socket"),
    ok.
