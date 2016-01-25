-module(saturn_proxy_vnode).
-behaviour(riak_core_vnode).
-include("saturn_leaf.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

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
         update/4,
         propagate/4,
         heartbeat/2,
         last_label/1,
         check_ready/1]).

-record(state, {partition,
                max_ts,
                last_label,
                myid}).

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%Testing purposes
last_label(Key) ->
    DocIdx = riak_core_util:chash_key({?BUCKET, Key}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_command(IndexNode,
                                        last_label,
                                        ?PROXY_MASTER).

read(Node, Key) ->
    riak_core_vnode_master:sync_command(Node,
                                        {read, Key},
                                        ?PROXY_MASTER).
heartbeat(Node, MyId) ->
    riak_core_vnode_master:command(Node,
                                   {heartbeat, MyId},
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).

update(Node, Key, Value, Clock) ->
    riak_core_vnode_master:sync_command(Node,
                                        {update, Key, Value, Clock},
                                        ?PROXY_MASTER).

propagate(Node, Key, Value, TimeStamp) ->
    riak_core_vnode_master:sync_command(Node,
                                        {propagate, Key, Value, TimeStamp},
                                        ?PROXY_MASTER).

init([Partition]) ->
    {ok, #state{partition=Partition,
                max_ts=0,
                last_label=none,
               }}.

%% @doc The table holding the prepared transactions is shared with concurrent
%%      readers, so they can safely check if a key they are reading is being updated.
%%      This function checks whether or not all tables have been intialized or not yet.
%%      Returns true if the have, false otherwise.
check_ready(Function) ->
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    PartitionList = chashbin:to_list(CHBin),
    check_ready_partition(PartitionList, Function).


check_ready_partition([], _Function) ->
    true;
check_ready_partition([{Partition, Node} | Rest], Function) ->
    Result = riak_core_vnode_master:sync_command({Partition, Node},
        Function,
        ?PROXY_MASTER,
        infinity),
    case Result of
        true ->
            check_ready_partition(Rest, Function);
        false ->
            false
    end.

handle_command({check_myid_ready}, _Sender, S0) ->
    S1 = check_myid(S0),
    {reply, true, S1};

handle_command({check_tables_ready}, _Sender, SD0) ->
    {reply, true, SD0};

handle_command({read, Key}, From, S0) ->
    {ok, Value} = ?BACKEND_CONNECTOR:read({Key, From}),
    {reply, {ok, Value}, S0};

handle_command({update, Key, Value, Clock}, _From, S0=#state{max_ts=MaxTS0, partition=Partition}) ->
    PhysicalClock = saturn_utilities:now_microsec(),
    TimeStamp = max(Clock, max(PhysicalClock, MaxTS0)),
    ok = ?BACKEND_CONNECTOR:update({Key, Value, TimeStamp}),
    Label = {Key, TimeStamp, {Partition, node()}},
    saturn_leaf_producer:new_label(MyId, Label, Partition),
    case groups_manager_serv:get_datanodes(Key) of
        {ok, Group} ->
            lists:foreach(fun({Host, Port}) ->
                            saturn_leaf_propagation_fsm_sup:start_fsm([Port, Host, {new_operation, Label, Key, Value}])
                          end, Group);
        {error, Reason} ->
            lager:error("No replication group for key: ~p (~p)", [Key, Reason])
    end,
    {reply, {ok, TimeStamp}, S0#state{max_ts=TimeStamp, last_label=Label}};
    {noreply, S0};

handle_command({propagate, Key, Value, TimeStamp}, _From, S0=#state{max_ts=MaxTS0}) ->
    MaxTS1 = max(TimeStamp, MaxTS0),
    ?BACKEND_CONNECTOR:propagation({Key, Value, TimeStamp}),
    {reply, ok, S0#state{max_ts=MaxTS1}};
    
handle_command({heartbeat, MyId}, _From, S0=#state{partition=Partition, max_ts=MaxTS0}) ->
    Clock = max(saturn_utilities:now_microsec(), MaxTS0),
    saturn_leaf_producer:partition_heartbeat(MyId, Partition, Clock),
    riak_core_vnode:send_command_after(?HEARTBEAT_FREQ, {heartbeat, MyId}),
    {noreply, S0#state{myid=MyId, max_ts=Clock}};

handle_command(last_label, _Sender, S0=#state{last_label=LastLabel}) ->
    {reply, {ok, LastLabel}, S0};

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

check_myid(S0) ->
    Value = riak_core_metadata:get(?MYIDPREFIX, ?MYIDKEY),
    case Value of
        undefined ->
            timer:sleep(100),
            check_myid(S0);
        MyId ->
            groups_manager_serv:set_myid(MyId),
            S0#state{myid=MyId}
    end.
