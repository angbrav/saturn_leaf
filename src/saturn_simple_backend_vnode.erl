-module(saturn_simple_backend_vnode).
-behaviour(riak_core_vnode).
-include("saturn_leaf.hrl").

-export([start_vnode/1,
         init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_exit/3]).

-export([read/2,
         update/3]).

-record(state, {partition,
                kv}).

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

read(Node, Key) ->
    riak_core_vnode_master:sync_command(Node,
                                        {read, Key},
                                        ?SIMPLE_MASTER).
update(Node, Key, Value) ->
    riak_core_vnode_master:sync_command(Node,
                                        {update, Key, Value},
                                        ?SIMPLE_MASTER).
init([Partition]) ->
    {ok, #state {partition=Partition,
                 kv=dict:new()
                 }}.

handle_command({read, Key}, _From, S0=#state{kv=KV}) ->
    case dict:find(Key, KV) of
        {ok, Value} ->
            {reply, {ok, Value}, S0};
        error ->
            {reply, {ok, empty}, S0}
    end;

handle_command({update, Key, Value}, _From, S0=#state{kv=KV0}) ->
    KV1 = dict:store(Key, Value, KV0),
    {reply, ok, S0#state{kv=KV1}};

%% Sample command: respond to a ping
handle_command(ping, _Sender, State) ->
    {reply, {pong, State#state.partition}, State};

handle_command(Message, _Sender, State) ->
    ?PRINT({unhandled_command, Message}),
    {noreply, State}.

handle_handoff_command(_Message, _Sender, State) ->
    {noreply, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(_Data, State) ->
    {reply, ok, State}.

encode_handoff_item(_ObjectName, _ObjectValue) ->
    <<>>.

is_empty(State) ->
    {true, State}.

delete(State) ->
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.
