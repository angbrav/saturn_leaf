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
         new_label/5,
         notify_update/3,
         check_tables_ready/0]).

-record(state, {partition,
                seq :: non_neg_integer(),
                dreads_uid :: dict(),
                dreads_counters :: dict(),
                label_uid :: dict(),
                buffer :: queue()}).

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

read(Node, ClientId, Key) ->
    riak_core_vnode_master:sync_command(Node,
                                        {read, ClientId, Key},
                                        ?PROXY_MASTER).
update(Node, ClientId, Key, Value) ->
    riak_core_vnode_master:sync_command(Node,
                                        {update, ClientId, Key, Value},
                                        ?PROXY_MASTER).
new_label(Node, UId, Label, Key, Value) ->
    riak_core_vnode_master:command(Node,
                                   {new_label, UId, Label, Key, Value},
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).

notify_update(Node, UId, Label) ->
    riak_core_vnode_master:command(Node,
                                   {update_completed, UId, Label},
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).
init([Partition]) ->
    {ok, #state {partition=Partition,
                 seq=0,
                 dreads_uid=dict:new(),
                 dreads_counters=dict:new(),
                 label_uid=dict:new(),
                 buffer=dict:new()}}.

%% @doc The table holding the prepared transactions is shared with concurrent
%%      readers, so they can safely check if a key they are reading is being updated.
%%      This function checks whether or not all tables have been intialized or not yet.
%%      Returns true if the have, false otherwise.
check_tables_ready() ->
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    PartitionList = chashbin:to_list(CHBin),
    check_table_ready(PartitionList).


check_table_ready([]) ->
    true;
check_table_ready([{Partition, Node} | Rest]) ->
    Result = riak_core_vnode_master:sync_command({Partition, Node},
        {check_tables_ready},
        ?PROXY_MASTER,
        infinity),
    case Result of
        true ->
            check_table_ready(Rest);
        false ->
            false
    end.

handle_command({check_tables_ready}, _Sender, SD0) ->
    {reply, true, SD0};

handle_command({read, ClientId, Key}, From, S0=#state{dreads_uid=DReadsUId0, dreads_counters=DReadsCounters0, seq=Seq0}) ->
    Seq1 = Seq0 + 1,
    UId = {ClientId, Seq1},
    S1=S0#state{seq=Seq1},
    case if_safe_read(ClientId, Key, S1) of
        true ->
            ?BACKEND_CONNECTOR_FSM:start_link(read, {Key, From}),
            {noreply, S1};
        {false, ConflictingUpdates} ->
            DReadsUId1 = lists:foldl(fun(ConflictingUId, Dict) ->
                                        dict:append(ConflictingUId, UId, Dict)
                                     end, DReadsUId0, ConflictingUpdates),
            DReadsCounters1 = dict:store(UId, {length(ConflictingUpdates), From, Key}, DReadsCounters0),
            {noreply, S1#state{dreads_counters=DReadsCounters1, dreads_uid=DReadsUId1}}
    end;

handle_command({update, ClientId, Key, Value}, _From, S0=#state{seq=Seq0, partition=Partition}) ->
    Seq1 = Seq0 + 1,
    UId = {ClientId, Seq1},
    S1=S0#state{seq=Seq1},
    S2 = buffer_update(UId, Key, Value, S1),
    saturn_leaf_producer:generate_label({Partition, node()}, UId, Key, Value),
    {reply, ok, S2};

handle_command({new_label, UId, Label, Key, Value}, _From, S0=#state{dreads_uid=_DReadsUId0, dreads_counters=_DReadsCounters0, buffer=Buffer0, label_uid=LabelUId0, partition=Partition}) ->
    {ClientId, _} = UId,
    case saturn_groups_manager:get_datanodes(Key) of
        {ok, Group} ->
            lists:foreach(fun({Host, Port}) ->
                            propagation_fsm_sup:start_fsm(Port, Host, {new_operation, Label, Key, Value})
                          end, Group);
        {error, Reason} ->
            lager:error("No replication group for key: ~p (~p)", [Key, Reason])
    end,
    case dict:find(ClientId, Buffer0) of
        {ok, Queue} ->
            case queue:peek(Queue) of
                {value, {UId, _, _}} ->
                    lager:info("Label received"),
                    ?BACKEND_CONNECTOR_FSM:start_link(update, {Key, Value, Label, UId, {Partition, node()}}),
                    {noreply, S0};
                _Other ->
                    LabelUId1 = dict:store(UId, Label, LabelUId0),
                    {noreply, S0#state{label_uid=LabelUId1}}
            end;
        error ->
            lager:error("There should be at least one entry in the queue"),
            {noreply, S0}
    end;

handle_command({update_completed, UId, Label}, _From, S0) ->
    saturn_leaf_producer:unblock_label(Label),
    S1 = process_buffer(UId, S0),
    S2 = process_pending_reads(UId, S1),
    {noreply, S2};

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
process_pending_reads(UId, S0=#state{dreads_uid=DReadsUId0, dreads_counters=DReadsCounters0}) ->
    case dict:find(UId, DReadsUId0) of
        {ok, ListReads} ->
            DReadsUId1 = dict:erase(UId, DReadsUId0),
            DReadsCounters1 = lists:foldl(fun(ReadUId, Dict) ->
                                            case Dict of
                                                error ->
                                                    error;
                                                _ ->
                                                    case dict:find(ReadUId, Dict) of
                                                        {ok, {Counter, From, Key}} ->
                                                            case Counter of
                                                                1 ->
                                                                    ?BACKEND_CONNECTOR_FSM:start_link(read, {Key, From}),
                                                                    dict:erase(ReadUId, Dict);
                                                                _ ->
                                                                    dict:store(ReadUId, {Counter-1, From, Key}, Dict)
                                                            end;
                                                        error ->
                                                            error
                                                    end
                                            end
                                          end, DReadsCounters0, ListReads),
            case DReadsCounters1 of
                error ->
                    lager:error("Error of consistency between dreads_uid and dreads_counters"),
                    S0;
                _ ->
                    S0#state{dreads_uid=DReadsUId1, dreads_counters=DReadsCounters1}
            end;
        error ->
            lager:info("No reads waiting for uid: ~p", [UId]),
            S0
    end.

process_buffer(UId, S0=#state{buffer=Buffer0, label_uid=LabelUId0, partition=Partition}) ->
    {ClientId, _} = UId,
    case dict:find(ClientId, Buffer0) of
        {ok, Queue0} ->
            case queue:peek(Queue0) of
                {value, {UId, _, _}} ->
                    Queue1 = queue:drop(Queue0),
                    case queue:is_empty(Queue1) of
                        false ->
                            Buffer1 = dict:store(ClientId, Queue1, Buffer0),
                            {value, {UIdNext, Key, Value}} = queue:peek(Queue1),
                            case dict:find(UIdNext, LabelUId0) of
                                {ok, Label} ->
                                    ?BACKEND_CONNECTOR_FSM:start_link(update, {Key, Value, Label, UIdNext, {Partition, node()}}),
                                    LabelUId1 = dict:erase(UIdNext, LabelUId0),
                                    S0#state{buffer=Buffer1, label_uid=LabelUId1};
                                error ->
                                    S0#state{buffer=Buffer1}
                            end;
                        true ->
                            Buffer1 = dict:erase(ClientId, Buffer0),
                            S0#state{buffer=Buffer1}
                    end;
                Other ->
                    lager:error("Entry in the queue does not match the recently executed operation or is empty: ~p", [Other]),
                    S0
            end;
        error ->
            lager:error("There should be at least one entry in the queue"),
            S0
    end.

if_safe_read(ClientId, Key, _S0=#state{buffer=Buffer}) ->
    case dict:find(ClientId, Buffer) of
        {ok, Queue} ->
            List = lists:foldl(fun(Update, List0) ->
                                {UId, KeyUpdate, _Value} = Update,
                                case KeyUpdate of
                                    Key ->
                                        List0 ++ [UId];
                                    _ ->
                                        List0
                                end
                               end, [], queue:to_list(Queue)),
            case length(List) of
                0 ->
                    true;
                _ ->
                    {false, List}
            end;
        error ->
            true
    end.

buffer_update(UId, Key, Value, S0=#state{buffer=Buffer0}) ->
    {ClientId, _Seq} = UId,
    case dict:find(ClientId, Buffer0) of
        {ok, Queue0} ->
            Queue1 = queue:in({UId, Key, Value}, Queue0);
        error ->
            Queue1 = queue:in({UId, Key, Value}, queue:new())
    end,
    Buffer1 = dict:store(ClientId, Queue1, Buffer0),
    S0#state{buffer=Buffer1}.

-ifdef(TEST).
if_safe_read_test() ->
    ClientId1 = clientid1,
    Key=3,
    Q1 = queue:in({1, Key, value}, queue:new()),
    Q2 = queue:in({2, 2, value}, Q1),
    Q3 = queue:in({3, Key, value}, Q2),
    D1 = dict:store(ClientId1, Q3, dict:new()),
    ?assertEqual({false, [1,3]}, if_safe_read(ClientId1, Key, #state{buffer=D1})),
    ?assertEqual({false, [2]}, if_safe_read(ClientId1, 2, #state{buffer=D1})),
    ?assertEqual(true, if_safe_read(ClientId1, 4, #state{buffer=D1})),
    ?assertEqual(true, if_safe_read(cid2, 4, #state{buffer=D1})).

buffer_update_test() ->
    UId1 = {client1, 2},
    UId2 = {client2, 5},
    %Test: Add operation to a client already in the buffer
    D1 = dict:store(client1, queue:in(whatever, queue:new()), dict:new()),
    #state{buffer=D2} = buffer_update(UId1, 4, 5, #state{buffer=D1}),
    {{value, Elem1}, Queue2}  = queue:out(dict:fetch(client1, D2)),
    ?assertEqual(whatever, Elem1),
    {{value, Elem2}, Queue3}  = queue:out(Queue2),
    ?assertEqual({UId1, 4, 5}, Elem2),
    ?assertEqual(true, queue:is_empty(Queue3)),

    %Test: Add operation to a client not present in the pbuffer
    #state{buffer=D3} = buffer_update(UId2, 4, 5, #state{buffer=D2}),
    {{value, Elem3}, Queue4}  = queue:out(dict:fetch(client2, D3)),
    ?assertEqual({UId2, 4, 5}, Elem3),
    ?assertEqual(true, queue:is_empty(Queue4)).

-endif.
