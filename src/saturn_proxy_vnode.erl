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
         async_read/4,
         update/4,
         async_update/5,
         propagate/4,
         heartbeat/1,
         init_proxy/2,
         remote_read/2,
         last_label/1,
         set_tree/4,
         set_groups/2,
         clean_state/1,
         set_receivers/2,
         data/4,
         collect_stats/3,
         check_ready/1]).

-record(state, {partition,
                max_ts,
                connector,
                last_label,
                manager,
                data,
                receivers,
                pending,
                staleness,
                myid}).

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%Testing purposes
last_label(BKey) ->
    DocIdx = riak_core_util:chash_key(BKey),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_command(IndexNode,
                                        last_label,
                                        ?PROXY_MASTER).

init_proxy(Node, MyId) ->
    riak_core_vnode_master:sync_command(Node,
                                        {init_proxy, MyId},
                                        ?PROXY_MASTER).

set_tree(Node, Paths, Tree, NLeaves) ->
    riak_core_vnode_master:sync_command(Node,
                                        {set_tree, Paths, Tree, NLeaves},
                                        ?PROXY_MASTER).

set_groups(Node, Groups) ->
    riak_core_vnode_master:sync_command(Node,
                                        {set_groups, Groups},
                                        ?PROXY_MASTER).

clean_state(Node) ->
    riak_core_vnode_master:sync_command(Node,
                                        clean_state,
                                        ?PROXY_MASTER).
    
read(Node, BKey, Clock) ->
    riak_core_vnode_master:sync_command(Node,
                                        {read, BKey, Clock},
                                        ?PROXY_MASTER).

async_read(Node, BKey, Clock, Client) ->
    riak_core_vnode_master:command(Node,
                                   {async_read, BKey, Clock, Client},
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).

heartbeat(Node) ->
    riak_core_vnode_master:command(Node,
                                   heartbeat,
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).

update(Node, BKey, Value, Clock) ->
    riak_core_vnode_master:sync_command(Node,
                                        {update, BKey, Value, Clock},
                                        ?PROXY_MASTER).

async_update(Node, BKey, Value, Clock, Client) ->
    riak_core_vnode_master:command(Node,
                                   {async_update, BKey, Value, Clock, Client},
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).

propagate(Node, TimeStamp, OriginNode, Sender) ->
    riak_core_vnode_master:command(Node,
                                   {propagate, TimeStamp, OriginNode, Sender},
                                   {fsm, undefines, self()},
                                   ?PROXY_MASTER).

remote_read(Node, Label) ->
    riak_core_vnode_master:command(Node,
                                   {remote_read, Label},
                                   {fsm, undefines, self()},
                                   ?PROXY_MASTER).

data(Node, TxId, BKey, Value) ->
    riak_core_vnode_master:command(Node,
                                   {data, TxId, BKey, Value},
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).

set_receivers(Node, Receivers) ->
    riak_core_vnode_master:sync_command(Node,
                                        {set_receivers, Receivers},
                                        ?PROXY_MASTER).

collect_stats(Node, From, Type) ->
    riak_core_vnode_master:sync_command(Node,
                                        {collect_stats, From, Type},
                                        ?PROXY_MASTER).

init([Partition]) ->
    Manager = groups_manager:init_state(integer_to_list(Partition)),
    Connector = ?BACKEND_CONNECTOR:connect([Partition]),
    Name1 = list_to_atom(integer_to_list(Partition) ++ "data"),
    Data = ets:new(Name1, [bag, named_table, private]),
    NameStaleness = list_to_atom(integer_to_list(Partition) ++ atom_to_list(staleness)),
    Staleness = ?STALENESS:init(NameStaleness),
    lager:info("Vnode init ~p", [self()]),
    {ok, #state{partition=Partition,
                max_ts=0,
                last_label=none,
                connector=Connector,
                data=Data,
                pending=none,
                staleness=Staleness,
                manager=Manager}}.

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

handle_command({check_tables_ready}, _Sender, SD0) ->
    {reply, true, SD0};

handle_command({collect_stats, From, Type}, _Sender, S0=#state{staleness=Staleness}) ->
    {reply, {ok, ?STALENESS:compute_raw(Staleness, From, Type)}, S0};

handle_command(clean_state, _Sender, S0=#state{connector=Connector0, partition=Partition, data=Data, staleness=Staleness0}) ->
    Connector1 = ?BACKEND_CONNECTOR:clean(Connector0, Partition),
    true = ets:delete_all_objects(Data),
    Name = list_to_atom(integer_to_list(Partition) ++ atom_to_list(staleness)),
    Staleness1 = ?STALENESS:clean(Staleness0, Name),
    {reply, ok, S0#state{max_ts=0,
                         last_label=none,
                         pending=none,
                         staleness=Staleness1,
                         connector=Connector1}};

handle_command({init_proxy, MyId}, _From, S0) ->
    {reply, ok, S0#state{myid=MyId}};

handle_command({set_receivers, Receivers}, _From, S0) ->
    {reply, ok, S0#state{receivers=Receivers}};

handle_command({set_tree, Paths, Tree, NLeaves}, _From, S0=#state{manager=Manager}) ->
    {reply, ok, S0#state{manager=Manager#state_manager{tree=Tree, paths=Paths, nleaves=NLeaves}}};

handle_command({set_groups, Groups}, _From, S0=#state{manager=Manager}) ->
    Table = Manager#state_manager.groups,
    ok = groups_manager:set_groups(Table, Groups),
    {reply, ok, S0};

handle_command({data, Id, BKey, Value}, _From, S0=#state{data=Data,
                                                         pending=Pending,
                                                         connector=Connector0,
                                                         myid=MyId,
                                                         staleness=Staleness0}) ->

    case Pending of
        {Id, Sender} ->
            {TimeStamp, _} = Id,
            {Connector1, Staleness1} = do_remote_update(BKey, Value, TimeStamp, Sender, MyId, Connector0, Staleness0),
            {noreply, S0#state{connector=Connector1, staleness=Staleness1, pending=none}};
        _ ->
            true = ets:insert(Data, {Id, {BKey, Value}}),
            {noreply, S0}
    end;

handle_command({read, BKey, Clock}, From, S0) ->
    case do_read(sync, BKey, Clock, From, S0) of
        {error, Reason} ->
            {reply, {error, Reason}, S0};
        {ok, Value} ->
            {reply, {ok, Value}, S0};
        {remote, S1} ->
            {reply, {ok, value}, S1}
            %{noreply, S1}
    end;

handle_command({async_read, BKey, Clock, Client}, _From, S0) ->
    case do_read(async, BKey, Clock, Client, S0) of
        {error, Reason} ->
            gen_server:reply(Client, {error, Reason}),
            {noreply, S0};
        {ok, Value} ->
            gen_server:reply(Client, {ok, Value}),
            {noreply, S0};
        {remote, S1} ->
            {noreply, S1}
    end;

handle_command({update, BKey, Value, Clock}, _From, S0) ->
    {{ok, TimeStamp}, S1} = do_update(BKey, Value, Clock, S0),
    {reply, {ok, TimeStamp}, S1};

handle_command({async_update, BKey, Value, Clock, Client}, _From, S0) ->
    {{ok, TimeStamp}, S1} = do_update(BKey, Value, Clock, S0),
    gen_server:reply(Client, {ok, TimeStamp}),
    {noreply, S1};

handle_command({propagate, TimeStamp, Node, Sender}, _From, S0=#state{connector=Connector0, myid=MyId, data=Data, staleness=Staleness0}) ->
    Id = {TimeStamp, Node},
    case ets:lookup(Data, Id) of
        [] ->
            {noreply, S0#state{pending={Id, Sender}}};
        [{Id, {BKey, Value}}] ->
            {Connector1, Staleness1} = do_remote_update(BKey, Value, TimeStamp, Sender, MyId, Connector0, Staleness0), 
            {noreply, S0#state{connector=Connector1, staleness=Staleness1}}
    end;
    
handle_command({remote_read, Label}, _From, S0=#state{max_ts=MaxTS0, myid=MyId, partition=Partition, connector=Connector, staleness=Staleness}) ->
    Sender = Label#label.sender,
    Clock = Label#label.timestamp,
    Staleness1 = ?STALENESS:add_remote(Staleness, Sender, Clock),
    BKeyToRead = Label#label.bkey,
    {ok, {Value, _Clock}} = ?BACKEND_CONNECTOR:read(Connector, {BKeyToRead}),
    PhysicalClock = saturn_utilities:now_microsec(),
    TimeStamp = max(PhysicalClock, MaxTS0+1),
    Payload = Label#label.payload,
    Client = Payload#payload_remote.client,
    Type = Payload#payload_remote.type_call,
    NewLabel = create_label(remote_reply, {routing, routing}, TimeStamp, {Partition, node()}, MyId, #payload_reply{value=Value, to=Sender, client=Client, type_call=Type}),
    saturn_leaf_producer:new_label(MyId, NewLabel, Partition, false),
    {noreply, S0#state{max_ts=TimeStamp, last_label=NewLabel, staleness=Staleness1}};

handle_command(heartbeat, _From, S0=#state{partition=Partition, max_ts=MaxTS0, myid=MyId}) ->
    Clock = max(saturn_utilities:now_microsec(), MaxTS0+1),
    case ((Clock - MaxTS0) > (?HEARTBEAT_FREQ*1000)) of
        true ->
            saturn_leaf_producer:partition_heartbeat(MyId, Partition, Clock),
            riak_core_vnode:send_command_after(?HEARTBEAT_FREQ, heartbeat),
            {noreply, S0#state{max_ts=Clock}};
        false ->
            riak_core_vnode:send_command_after(?HEARTBEAT_FREQ, heartbeat),
            {noreply, S0}
    end;

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

create_label(Operation, BKey, TimeStamp, Node, Id, Payload) ->
    #label{operation=Operation,
           bkey=BKey,
           timestamp=TimeStamp,
           node=Node,
           sender=Id,
           payload=Payload
           }.

do_read(Type, BKey, Clock, From, S0=#state{myid=MyId, max_ts=MaxTS0, partition=Partition, connector=Connector, manager=Manager}) ->
    Groups = Manager#state_manager.groups,
    Tree = Manager#state_manager.tree,
    case groups_manager:get_closest_dcid(BKey, Groups, MyId, Tree) of
        {ok, MyId} ->    
            ?BACKEND_CONNECTOR:read(Connector, {BKey});
        {ok, Id} ->
            %Remote read
            PhysicalClock = saturn_utilities:now_microsec(),
            TimeStamp = max(Clock, max(PhysicalClock, MaxTS0)),
            Label = create_label(remote_read, BKey, TimeStamp, {Partition, node()}, MyId, #payload_remote{to=Id, client=From, type_call=Type}),
            saturn_leaf_producer:new_label(MyId, Label, Partition, false),    
            {remote, S0#state{max_ts=TimeStamp, last_label=Label}};
        {error, Reason} ->
            lager:error("BKey ~p ~p in the dictionary",  [BKey, Reason]),
            {error, Reason}
    end.
    
do_update(BKey, Value, Clock, S0=#state{max_ts=MaxTS0, partition=Partition, myid=MyId, connector=Connector0, manager=Manager, receivers=Receivers}) ->
    PhysicalClock = saturn_utilities:now_microsec(),
    TimeStamp = max(Clock+1, max(PhysicalClock, MaxTS0+1)),
    S1 = case groups_manager:do_replicate(BKey, Manager#state_manager.groups, MyId) of
        true ->
            {ok, Connector1} = ?BACKEND_CONNECTOR:update(Connector0, {BKey, Value, TimeStamp}),
            S0#state{connector=Connector1};
        false ->
            S0;
        {error, Reason1} ->
            lager:error("BKey ~p ~p in the dictionary",  [BKey, Reason1]),
            S0
    end,
    Label = create_label(update, BKey, TimeStamp, {Partition, node()}, MyId, {}),
    saturn_leaf_producer:new_label(MyId, Label, Partition, true),
    case groups_manager:get_datanodes_ids(BKey, Manager#state_manager.groups, MyId) of
        {ok, Group} ->
            lists:foreach(fun(Id) ->
                            Receiver = dict:fetch(Id, Receivers),
                            UId = {TimeStamp, {Partition, node()}},
                            saturn_data_receiver:data(Receiver, UId, BKey, Value)
                          end, Group);
        {error, Reason2} ->
            lager:error("No replication group for bkey: ~p (~p)", [BKey, Reason2])
    end,
    {{ok, TimeStamp}, S1#state{max_ts=TimeStamp, last_label=Label}}.

do_remote_update(BKey, Value, TimeStamp, Sender, MyId, Connector0, Staleness0) ->
    Staleness1 = ?STALENESS:add_update(Staleness0, Sender, TimeStamp),
    {ok, {_, Clock}} = ?BACKEND_CONNECTOR:read(Connector0, {BKey}),
    case (Clock<TimeStamp) of
        true ->
            {ok, Connector1} = ?BACKEND_CONNECTOR:update(Connector0, {BKey, Value, TimeStamp}),
            saturn_leaf_converger:handle(MyId, completed),
            {Connector1, Staleness1};
        false ->
            saturn_leaf_converger:handle(MyId, completed),
            {Connector0, Staleness1}
    end.
