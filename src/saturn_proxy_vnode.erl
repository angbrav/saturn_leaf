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

-export([read/3,
         update/4,
         propagate/4,
         heartbeat/2,
         remote_read/2,
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

read(Node, Key, Clock) ->
    riak_core_vnode_master:sync_command(Node,
                                        {read, Key, Clock},
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

remote_read(Node, Label) ->
    riak_core_vnode_master:sync_command(Node,
                                        {remote_read, Label},
                                        ?PROXY_MASTER).


init([Partition]) ->
    {ok, #state{partition=Partition,
                max_ts=0,
                last_label=none
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

handle_command({read, Key, Clock}, From, S0=#state{myid=MyId, max_ts=MaxTS0, partition=Partition}) ->
    case groups_manager_serv:do_replicate(Key) of
        true ->    
            {ok, Value} = ?BACKEND_CONNECTOR:read({Key}),
            {reply, {ok, Value}, S0};
        false ->
            %Remote read
            PhysicalClock = saturn_utilities:now_microsec(),
            TimeStamp = max(Clock, max(PhysicalClock, MaxTS0)),
            {ok, KeySource} = groups_manager_serv:get_key_sample(),
            Label = create_label(remote_read, Key, TimeStamp, {Partition, node()}, MyId, #payload_remote{to=all, key_source=KeySource, client=From}),
            saturn_leaf_producer:new_label(MyId, Label, Partition),    
            {noreply, S0#state{max_ts=TimeStamp, last_label=Label}};
        {error, Reason} ->
            lager:error("Key ~p ~p in the dictionary",  [Key, Reason]),
            {reply, {error, Reason}, S0}
    end;

handle_command({update, Key, Value, Clock}, _From, S0=#state{max_ts=MaxTS0, partition=Partition, myid=MyId}) ->
    PhysicalClock = saturn_utilities:now_microsec(),
    TimeStamp = max(Clock+1, max(PhysicalClock, MaxTS0+1)),
    case groups_manager_serv:do_replicate(Key) of
        true ->
            ok = ?BACKEND_CONNECTOR:update({Key, Value, TimeStamp});
        false ->
            noop;
        {error, Reason1} ->
            lager:error("Key ~p ~p in the dictionary",  [Key, Reason1])
    end,
    Label = create_label(update, Key, TimeStamp, {Partition, node()}, MyId, {}),
    saturn_leaf_producer:new_label(MyId, Label, Partition),
    case groups_manager_serv:get_datanodes(Key) of
        {ok, Group} ->
            lists:foreach(fun({Host, Port}) ->
                            saturn_leaf_propagation_fsm_sup:start_fsm([Port, Host, {new_operation, Label, Value}])
                          end, Group);
        {error, Reason2} ->
            lager:error("No replication group for key: ~p (~p)", [Key, Reason2])
    end,
    {reply, {ok, TimeStamp}, S0#state{max_ts=TimeStamp, last_label=Label}};

handle_command({propagate, Key, Value, _TimeStamp}, _From, S0) ->
    ok = ?BACKEND_CONNECTOR:update({Key, Value, 0}),
    {reply, ok, S0};
    
handle_command({remote_read, Label}, _From, S0=#state{max_ts=MaxTS0, myid=MyId, partition=Partition}) ->
    KeyToRead = Label#label.key,
    {ok, {Value, _Clock}} = ?BACKEND_CONNECTOR:read({KeyToRead}),
    PhysicalClock = saturn_utilities:now_microsec(),
    TimeStamp = max(PhysicalClock, MaxTS0+1),
    Payload = Label#label.payload,
    Key = Payload#payload_remote.key_source,
    Client = Payload#payload_remote.client,
    Source = Label#label.sender,
    Label = create_label(remote_reply, Key, TimeStamp, {Partition, node()}, MyId, #payload_reply{value=Value, to=Source, client=Client}),
    saturn_leaf_producer:new_label(MyId, Label, Partition),
    {reply, ok, S0#state{max_ts=TimeStamp, last_label=Label}};

handle_command({heartbeat, MyId}, _From, S0=#state{partition=Partition, max_ts=MaxTS0}) ->
    Clock = max(saturn_utilities:now_microsec(), MaxTS0+1),
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
    
create_label(Operation, Key, TimeStamp, Node, Id, Payload) ->
    #label{operation=Operation,
           key=Key,
           timestamp=TimeStamp,
           node=Node,
           sender=Id,
           payload=Payload
           }.
