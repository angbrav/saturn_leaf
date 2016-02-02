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
-module(saturn_leaf_tcp_connection_handler_fsm).
-behaviour(gen_fsm).

-record(state, {socket, server, myid}).

-export([start_link/3]).
-export([init/1,
         code_change/4,
         handle_event/3,
         handle_info/3,
         handle_sync_event/4,
         terminate/3]).
-export([receive_message/2,
         close_socket/2
        ]).

%% ===================================================================
%% Public API
%% ===================================================================

start_link(Socket, Server, MyId) ->
    gen_fsm:start_link(?MODULE, [Socket, Server, MyId], []).


%% ===================================================================
%% gen_fsm callbacks
%% ===================================================================

init([Socket, Server, MyId]) ->
    {ok, receive_message, #state{socket=Socket, server=Server, myid=MyId},0}.


receive_message(timeout, State=#state{socket=Socket, server=Server, myid=MyId}) ->
    case gen_tcp:recv(Socket, 0) of
        {ok, Bin} ->
            Message = binary_to_term(Bin),
            ok = Server:handle(MyId, Message),
            case gen_tcp:send(Socket, term_to_binary(acknowledge)) of
			    ok ->
			        ok;
			    {error,Reason} ->
			        lager:error("Could not send ack, reason ~p", [Reason])
		    end;
        {error, Reason} ->
            lager:error("Problem with the socket, reason: ~p", [Reason])
    end,
    {next_state, close_socket,State,0}.

close_socket(timeout, State=#state{socket=Socket}) ->
    gen_tcp:close(Socket),
    {stop, normal, State}.

handle_info(Message, _StateName, StateData) ->
    lager:error("Recevied info:  ~p",[Message]),
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.
