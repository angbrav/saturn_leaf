-module(saturn_proxy_vnode).
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

-export([read/3,
         update/4,
         new_label/5,
         notify_update/3]).

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

handle_command({read, ClientId, Key}, From, S0=#state{dreads_uid=DReadsUId0, dreads_counters=DReadsCounters0, seq=Seq0}) ->
    Seq1 = Seq0 + 1,
    UId = {ClientId, Seq1},
    S1=S0#state{seq=Seq1},
    case if_safe_read(ClientId, Key, S1) of
        true ->
            ?BACKEND_CONNECTOR_FSM:start_fsm([read, {Key, From}]),
            {noreply, S1};
        {false, ConflictingUpdates} ->
            DReadsUId1 = lists:foldl(fun(ConflictingUId, Dict) ->
                                        case dict:find(ConflictingUId, Dict) of
                                           {ok, List0} ->
                                               List1 =  List0 ++ [UId];
                                           error ->
                                               List1 = [UId]
                                        end,
                                        dict:store(ConflictingUId, List1, Dict)
                                     end, DReadsUId0, ConflictingUpdates),
            DReadsCounters1 = dict:store(UId, {length(ConflictingUpdates), From, Key}, DReadsCounters0),
            {noreply, S1#state{dreads_counters=DReadsCounters1, dreads_uid=DReadsUId1}}
    end;

handle_command({update, ClientId, Key, Value}, _From, S0=#state{seq=Seq0}) ->
    Seq1 = Seq0 + 1,
    UId = {ClientId, Seq1},
    S1=S0#state{seq=Seq1},
    S2 = buffer_update(UId, Key, Value, S1),
    saturn_leaf_producer:generate_label(UId, Key, Value),
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
                {UId, _} ->
                    ?BACKEND_CONNECTOR_FSM:start_fsm([update, {Key, Value, Label, UId, {Partition, node()}}]);
                _ ->
                    LabelUId1 = dict:store(UId, Label, LabelUId0),
                    {noreply, S0#state{label_uid=LabelUId1}}
            end;
        error ->
            lager:error("There should be at least one entry in the queue")
    end;

handle_command({update_completed, UId, Label}, _From, S0=#state{dreads_uid=DReadsUId0, dreads_counters=DReadsCounters0, buffer=Buffer0, label_uid=LabelUId0, partition=Partition}) ->
    {ClientId, _} = UId,
    saturn_leaf_producer:unblock_label(Label),
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
                                                                    ?BACKEND_CONNECTOR_FSM:start_fsm([read, {Key, From}]),
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
                    {noreply, S0};
                _ ->
                    case dict:find(ClientId, Buffer0) of
                        {ok, Queue0} ->
                            case queue:peek(Queue0) of
                                {UId, _} ->
                                    Queue1 = queue:drop(Queue0),
                                    case queue:is_empty(Queue1) of
                                        false ->
                                            {UIdNext, Key, Value} = queue:peek(Queue1),
                                            case dict:find(UIdNext, LabelUId0) of
                                                Label ->
                                                    ?BACKEND_CONNECTOR_FSM:start_fsm([update, {Key, Value, Label, UIdNext, {Partition, node()}}]),
                                                    LabelUId1 = dict:erase(UIdNext, LabelUId0),
                                                    {noreply, S0#state{dreads_uid=DReadsUId1, dreads_counters=DReadsCounters1, label_uid=LabelUId1}};
                                                error ->
                                                    {noreply, S0#state{dreads_uid=DReadsUId1, dreads_counters=DReadsCounters1}}
                                            end;
                                        true ->
                                            {noreply, S0#state{dreads_uid=DReadsUId1, dreads_counters=DReadsCounters1}}
                                    end;
                                _ ->
                                    lager:error("Entry in the queue does not match the recently executed operation"),
                                    {noreply, S0}
                            end;
                        error ->
                            lager:error("There should be at least one entry in the queue"),
                            {noreply, S0}
                    end
            end;
        error ->
            lager:error("Executed update is not in the delayes updates mapped by uid data dict"),
            {noreply, S0}
    end;
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

if_safe_read(ClientId, Key, _S0=#state{buffer=Buffer}) ->
    case dict:find(ClientId, Buffer) of
        {ok, Queue} ->
            List = lists:foldl(fun(Update, List0) ->
                                {UId, {KeyUpdate, _Value}} = Update,
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
