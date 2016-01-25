-module(riak_connector_fsm).
-behaviour(gen_fsm).

-include("saturn_leaf.hrl").

-export([start_link/2]).
        
-export([init/1,
         code_change/4,
         handle_event/3,
         handle_info/3,
         terminate/3,
         handle_sync_event/4]).
-export([select_operation/2,
         update/2,
         read/2,
         propagation/2,
         stop/2
        ]).

-record(state, {operation, payload, reason, riak}).

start_link(Operation, Payload) ->
    gen_fsm:start_link(?MODULE, [Operation, Payload], []).

%% ===================================================================
%% gen_fsm callbacks
%% ===================================================================

init([Operation, Payload]) ->
    {ok, select_operation, #state{operation=Operation,
                                  payload=Payload
                                 }, 0}.

select_operation(timeout, S0=#state{operation=Operation}) ->
    {ok, Pid} = riakc_pb_socket:start_link(?RIAK_NODE, ?RIAK_PORT).
    S1 = S0#state{riak=Pid},
    case Operation of
        update ->
            {next_state, update, S1, 0};
        read ->
            {next_state, read, S1, 0};
        propagation ->
            {next_state, propagation, S1, 0};
        Other ->
            lager:error("Wrong operation type: ~p", [Other])
    end.

update(timeout, State=#state{payload=Payload, riak=Riak})->
    lager:info("Connector received an update"),
    {Key, Value, TimeStamp, Seq} = Payload,
    DocIdx = riak_core_util:chash_key({?BUCKET, Key}),
    PrefListProxy = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
    [{IndexNodeProxy, _TypeProxy}] = PrefListProxy,
    saturn_proxy_vnode:update_completed(IndexNodeProxy, Key, Value, TimeStamp, Seq),
    {next_state, stop, State#state{reason=normal},0}.

read(timeout, State=#state{payload=Payload, riak=Riak})->
    {Key, Client} = Payload,
    DocIdx = riak_core_util:chash_key({?BUCKET, Key}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?SIMPLE_SERVICE),
    [{IndexNode, _Type}] = PrefList,
    {ok, Value} = saturn_simple_backend_vnode:read(IndexNode, Key),
    riak_core_vnode:reply(Client, {ok, Value}),
    {next_state, stop, State#state{reason=normal},0}.

propagation(timeout, State=#state{payload=Payload, riak=Riak})->
    {Key, Value, TimeStamp} = Payload,
    DocIdx = riak_core_util:chash_key({?BUCKET, Key}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?SIMPLE_SERVICE),
    [{IndexNode, _Type}] = PrefList,
    ok = saturn_simple_backend_vnode:propagation(IndexNode, Key, {Value, TimeStamp}),
    {next_state, stop, State#state{reason=normal},0}.

stop(timeout, State=#state{reason=Reason}) ->
    {stop, Reason, State}.

handle_info(Message, _StateName, StateData) ->
    lager:error("Unexpected message: ~p",[Message]),
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_,_,_) -> ok.
