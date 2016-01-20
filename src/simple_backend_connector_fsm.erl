-module(simple_backend_connector_fsm).
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

-record(state, {operation, payload, reason}).

start_link(Operation, Payload) ->
    gen_fsm:start_link(?MODULE, [Operation, Payload], []).

%% ===================================================================
%% gen_fsm callbacks
%% ===================================================================

init([Operation, Payload]) ->
    {ok, select_operation, #state{operation=Operation,
                                  payload=Payload
                                 }, 0}.

select_operation(timeout, State=#state{operation=Operation}) ->
    case Operation of
        update ->
            {next_state, update, State, 0};
        read ->
            {next_state, read, State, 0};
        propagation ->
            {next_state, propagation, State, 0};
        Other ->
            lager:error("Wrong operation type: ~p", [Other])
    end.

update(timeout, State=#state{payload=Payload})->
    lager:info("Connector received an update"),
    {Key, Value, TimeStamp, Seq} = Payload,
    DocIdx = riak_core_util:chash_key({?BUCKET, Key}),
    PrefListStore = riak_core_apl:get_primary_apl(DocIdx, 1, ?SIMPLE_SERVICE),
    PrefListProxy = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
    [{IndexNodeStore, _TypeStore}] = PrefListStore,
    [{IndexNodeProxy, _TypeProxy}] = PrefListProxy,
    ok = saturn_simple_backend_vnode:update(IndexNodeStore, Key, {Value, TimeStamp}),
    saturn_proxy_vnode:update_completed(IndexNodeProxy, Key, Value, TimeStamp, Seq),
    {next_state, stop, State#state{reason=normal},0}.

read(timeout, State=#state{payload=Payload})->
    {Key, Client} = Payload,
    DocIdx = riak_core_util:chash_key({?BUCKET, Key}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?SIMPLE_SERVICE),
    [{IndexNode, _Type}] = PrefList,
    {ok, Value} = saturn_simple_backend_vnode:read(IndexNode, Key),
    riak_core_vnode:reply(Client, {ok, Value}),
    {next_state, stop, State#state{reason=normal},0}.

propagation(timeout, State=#state{payload=Payload})->
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
