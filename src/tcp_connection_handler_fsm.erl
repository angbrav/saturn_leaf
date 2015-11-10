-module(tcp_connection_handler_fsm).
-behaviour(gen_fsm).

-record(state, {socket, server}).

-export([start_link/2]).
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

start_link(Socket, Server) ->
    gen_fsm:start_link(?MODULE, [Socket, Server], []).


%% ===================================================================
%% gen_fsm callbacks
%% ===================================================================

init([Socket, Server]) ->
    {ok, receive_message, #state{socket=Socket, server=Server},0}.


receive_message(timeout, State=#state{socket=Socket, server=Server}) ->
    case gen_tcp:recv(Socket, 0) of
        {ok, Bin} ->
            Message = binary_to_term(Bin),
            ok = Server:handle(Message),
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
