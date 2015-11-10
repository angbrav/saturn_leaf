-module(saturn_tcp_recv_fsm).
-behaviour(gen_fsm).

-record(state, {port, handler, listener}). % the current socket

-export([start_link/2]).
-export([init/1,
         code_change/4,
         handle_event/3,
         handle_info/3,
         handle_sync_event/4,
         terminate/3]).
-export([accept/2
        ]).

-define(TIMEOUT,10000).

start_link(Port, Handler) ->
    gen_fsm:start_link(?MODULE, [Port, Handler], []).

init([Port, Handler]) ->
    {ok, ListenSocket} = gen_tcp:listen(
                           Port,
                           [{active,false}, binary,
                            {packet,4},{reuseaddr, true}
                           ]),
    {ok, accept, #state{port=Port, listener=ListenSocket, handler=Handler},0}.

%% Accepts an incoming tcp connection and spawn and new fsm 
%% to process the connection, so that new connections could 
%% be processed in parallel
accept(timeout, State=#state{listener=ListenSocket, handler=Handler}) ->
    {ok, AcceptSocket} = gen_tcp:accept(ListenSocket),
    {ok, _} = tcp_connection_handler_fsm_sup:start_fsm([AcceptSocket, Handler]),
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
