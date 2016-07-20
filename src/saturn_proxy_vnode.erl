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
         fsm_read/4,
         fsm_idle/2,
         async_txread/4,
         update/4,
         async_update/5,
         propagate/5,
         heartbeat/1,
         init_proxy/2,
         remote_read/2,
         last_label/1,
         set_tree/4,
         set_groups/2,
         data/4,
         clean_state/1,
         set_receivers/2,
         remote_prepare/4,
         write_fsm_idle/2,
         async_txwrite/4,
         prepare/5,
         remote_prepare/5,
         commit/3,
         remote_write_tx/4,
         remote_fsm_idle/2,
         propagate_remote/3,
         collect_stats/3,
         check_ready/1]).

-record(state, {partition,
                max_ts,
                connector,
                last_label,
                read_fsms,
                pending_readtxs,
                write_fsms,
                remote_fsms,
                pending_writetxs,
                pending_remotetxs,
                pending_reads,
                manager,
                receivers,
                prepared_tx,
                key_prepared,
                pending_counter,
                read_id,
                data,
                remote,
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

async_txread(Node, BKeys, Clock, Client) ->
    riak_core_vnode_master:command(Node,
                                   {async_txread, BKeys, Clock, Client},
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).

fsm_read(Node, BKey, Clock, Fsm) ->
    riak_core_vnode_master:command(Node,
                                   {fsm_read, BKey, Clock, Fsm},
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).

fsm_idle(Node, Fsm) ->
    riak_core_vnode_master:command(Node,
                                   {fsm_idle, Fsm},
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).

heartbeat(Node) ->
    riak_core_vnode_master:command(Node,
                                   heartbeat,
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).

data(Node, TxId, BKey, Value) ->
    riak_core_vnode_master:command(Node,
                                   {data, TxId, BKey, Value},
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).

remote_prepare(Node, TxId, Number, TimeStamp) ->
    riak_core_vnode_master:command(Node,
                                   {remote_prepare, TxId, Number, TimeStamp},
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

propagate(Node, BKey, TimeStamp, SenderId, OriginNode) ->
    riak_core_vnode_master:command(Node,
                                   {propagate, BKey, TimeStamp, SenderId, OriginNode},
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).

remote_read(Node, Label) ->
    riak_core_vnode_master:command(Node,
                                   {remote_read, Label},
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).

set_receivers(Node, Receivers) ->
    riak_core_vnode_master:sync_command(Node,
                                        {set_receivers, Receivers},
                                        ?PROXY_MASTER).

write_fsm_idle(Node, Fsm) ->
    riak_core_vnode_master:command(Node,
                                   {write_fsm_idle, Fsm},
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).

async_txwrite(Node, BKeyValuePairs, Clock, Client) ->
    riak_core_vnode_master:command(Node,
                                   {async_txwrite, BKeyValuePairs, Clock, Client},
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).

prepare(Node, TxId, Pairs, TimeStamp, Fsm) ->
    riak_core_vnode_master:command(Node,
                                   {prepare, TxId, Pairs, TimeStamp, Fsm},
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).

remote_prepare(Node, TxId, Number, TimeStamp, Fsm) ->
    riak_core_vnode_master:command(Node,
                                   {remote_prepare, TxId, Number, TimeStamp, Fsm},
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).

commit(Node, TxId, Remote) ->
    riak_core_vnode_master:command(Node,
                                   {commit, TxId, Remote},
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).

remote_write_tx(Node, Origin, BKeys, TimeStamp) ->
    riak_core_vnode_master:command(Node,
                                   {remote_write_tx, Origin, BKeys, TimeStamp},
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).

remote_fsm_idle(Node, Fsm) ->
    riak_core_vnode_master:command(Node,
                                   {remote_fsm_idle, Fsm},
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).

propagate_remote(Node, TxId, Pairs) ->
    riak_core_vnode_master:command(Node,
                                   {propagate_remote, TxId, Pairs},
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).

collect_stats(Node, From, Type) ->
    riak_core_vnode_master:sync_command(Node,
                                        {collect_stats, From, Type},
                                        ?PROXY_MASTER).

init_prop_fsms(_Vnode, _MyId, Reads0, Writes0, Remotes0, 0) ->
    {ok, Reads0, Writes0, Remotes0};

init_prop_fsms(Vnode, MyId, Reads0, Writes0, Remotes0, Rest) ->
    {ok, ReadFsmRef} = read_tx_coord_fsm:start_link(Vnode),
    {ok, WriteFsmRef} = write_tx_coord_fsm:start_link(Vnode),
    {ok, RemoteFsmRef} = remote_write_tx_coord_fsm:start_link(Vnode, MyId),
    Reads1 = queue:in(ReadFsmRef, Reads0),
    Writes1 = queue:in(WriteFsmRef, Writes0),
    Remotes1 = queue:in(RemoteFsmRef, Remotes0),
    init_prop_fsms(Vnode, MyId, Reads1, Writes1, Remotes1, Rest-1).


init([Partition]) ->
    Manager = groups_manager:init_state(integer_to_list(Partition)),
    Connector = ?BACKEND_CONNECTOR:connect([Partition]),
    Name1 = list_to_atom(integer_to_list(Partition) ++ "pending_readtx"),
    Name2 = list_to_atom(integer_to_list(Partition) ++ "pending_writetx"),
    Name3 = list_to_atom(integer_to_list(Partition) ++ "preparedtx"),
    Name4 = list_to_atom(integer_to_list(Partition) ++ "keyprepared"),
    Name5 = list_to_atom(integer_to_list(Partition) ++ "pending_reads"),
    Name6 = list_to_atom(integer_to_list(Partition) ++ "data"),
    Name7 = list_to_atom(integer_to_list(Partition) ++ "pending_remotetx"),
    Name8 = list_to_atom(integer_to_list(Partition) ++ "pending_counter"),
    PreparedTx = ets:new(Name3, [set, named_table, private]),
    KeyPrepared = ets:new(Name4, [set, named_table, private]),
    PendingReads = ets:new(Name5, [set, named_table, private]),
    Data = ets:new(Name6, [bag, named_table, private]),
    PendingCounter = ets:new(Name8, [set, named_table, private]),
    NameStaleness = list_to_atom(integer_to_list(Partition) ++ atom_to_list(staleness)),
    Staleness = ?STALENESS:init(NameStaleness),
    lager:info("Vnode init"),
    {ok, #state{partition=Partition,
                max_ts=0,
                last_label=none,
                pending_readtxs=ets_queue:new(Name1),
                pending_writetxs=ets_queue:new(Name2),
                pending_remotetxs=ets_queue:new(Name7),
                connector=Connector,
                prepared_tx=PreparedTx,
                key_prepared=KeyPrepared,
                pending_reads=PendingReads,
                remote=dict:new(),
                data=Data,
                staleness=Staleness,
                pending_counter=PendingCounter,
                read_id=0,
                manager=Manager}}.

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

handle_command({set_receivers, Receivers}, _From, S0) ->
    {reply, ok, S0#state{receivers=Receivers}};

handle_command({collect_stats, From, Type}, _Sender, S0=#state{staleness=Staleness}) ->
    {reply, {ok, ?STALENESS:compute_raw(Staleness, From, Type)}, S0};

handle_command(clean_state, _Sender, S0=#state{connector=Connector0,
                                               partition=Partition,
                                               pending_readtxs=PendingReadTxs,
                                               pending_writetxs=PendingWriteTxs,
                                               prepared_tx=PreparedTx0,
                                               key_prepared=KeyPrepared0,
                                               pending_reads=PendingReads0,
                                               pending_remotetxs=PendingRemoteTxs,
                                               staleness=Staleness,
                                               pending_counter=PendingCounter}) ->
    Connector1 = ?BACKEND_CONNECTOR:clean(Connector0, Partition),
    true = ets:delete_all_objects(PreparedTx0),
    true = ets:delete_all_objects(KeyPrepared0),
    true = ets:delete_all_objects(PendingReads0),
    true = ets:delete_all_objects(PendingCounter),
    Name = list_to_atom(integer_to_list(Partition) ++ atom_to_list(staleness)),
    Staleness1 = ?STALENESS:clean(Staleness, Name),
    {reply, ok, S0#state{max_ts=0,
                         last_label=none,
                         pending_readtxs=ets_queue:clean(PendingReadTxs),
                         pending_writetxs=ets_queue:clean(PendingWriteTxs),
                         pending_remotetxs=ets_queue:clean(PendingRemoteTxs),
                         read_id=0,
                         staleness=Staleness1,
                         connector=Connector1}};

handle_command({init_proxy, MyId}, _From, S0=#state{partition=Partition}) ->
    {ok, ReadFsms, WriteFsms, RemoteFsms} = init_prop_fsms({Partition, node()}, MyId, queue:new(), queue:new(), queue:new(), ?N_FSMS),
    {reply, ok, S0#state{myid=MyId,
                         read_fsms=ReadFsms,
                         write_fsms=WriteFsms,
                         remote_fsms=RemoteFsms}};

handle_command({set_tree, Paths, Tree, NLeaves}, _From, S0=#state{manager=Manager}) ->
    {reply, ok, S0#state{manager=Manager#state_manager{tree=Tree, paths=Paths, nleaves=NLeaves}}};

handle_command({set_groups, Groups}, _From, S0=#state{manager=Manager}) ->
    Table = Manager#state_manager.groups,
    ok = groups_manager:set_groups(Table, Groups),
    {reply, ok, S0};

handle_command({read, BKey, Clock}, From, S0) ->
    case do_read(sync, BKey, Clock, From, S0) of
        {error, Reason} ->
            {reply, {error, Reason}, S0};
        {ok, Value} ->
            {reply, {ok, Value}, S0};
        {remote, S1} ->
            {noreply, S1}
    end;

handle_command({data, TxId, BKey, Value}, _From, S0=#state{data=Data,
                                                           remote=Remote0,
                                                           connector=Connector0,
                                                           key_prepared=KeyPrepared,
                                                           partition=Partition,
                                                           myid=MyId,
                                                           staleness=Staleness,
                                                           prepared_tx=PreparedTx}) ->
    %lager:info("I have received remote update: TxId ~p, BKey ~p, Value ~p", [TxId, BKey, Value]),
    case dict:find(TxId, Remote0) of
        {ok, {1, Fsm}} ->
            {TimeStamp, _Node} = TxId,
            true = ets:insert(Data, {TxId, {BKey, Value}}),
            List = ets:lookup(Data, TxId), 
            Remote1 = do_remote_prepare(TxId, TimeStamp, List, Data, Remote0, KeyPrepared, PreparedTx),
            gen_fsm:send_event(Fsm, {prepared, {Partition, node()}}),
            {noreply, S0#state{remote=Remote1}};
        {ok, {single, Sender}} ->
            {TimeStamp, _Node} = TxId,
            Staleness1 = ?STALENESS:add_update(Staleness, Sender, TimeStamp),
            {ok, Connector1} = ?BACKEND_CONNECTOR:update(Connector0, {BKey, Value, TimeStamp}),
            Remote1 = dict:erase(TxId, Remote0),
            saturn_leaf_converger:handle(MyId, {completed, TimeStamp}),
            {noreply, S0#state{connector=Connector1, remote=Remote1, staleness=Staleness1}};
        {ok, {Left, Fsm}} ->
            Remote1 = dict:store(TxId, {Left-1, Fsm}, Remote0),
            {noreply, S0#state{remote=Remote1}};
        error ->
            true = ets:insert(Data, {TxId, {BKey, Value}}),
            {noreply, S0}
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

handle_command({async_txread, BKeys, Clock, Client}, _From, S0=#state{read_fsms=ReadFsms0, pending_readtxs=PendingReadTxs0, max_ts=MaxTS0}) ->
    case queue:out(ReadFsms0) of
        {{value, Idle}, ReadFsms1} ->
            PhysicalClock = saturn_utilities:now_microsec(),
            ReadTime = max(Clock, max(PhysicalClock, MaxTS0)),
            gen_fsm:send_event(Idle, {new_tx, BKeys, ReadTime, Client}),
            {noreply, S0#state{max_ts=ReadTime, read_fsms=ReadFsms1}};
        {empty, ReadFsms0} ->
            PendingReadTxs1 = ets_queue:in({BKeys, Clock, Client}, PendingReadTxs0),
            {noreply, S0#state{pending_readtxs=PendingReadTxs1}}
    end;

handle_command({fsm_read, BKey, Clock, Fsm}, _From, S0=#state{myid=MyId,
                                                              max_ts=MaxTS0,
                                                              partition=Partition,
                                                              connector=Connector,
                                                              manager=Manager,
                                                              key_prepared=KeyPrepared,
                                                              pending_counter=PendingCounter,
                                                              pending_reads=PendingReads,
                                                              read_id=ReadId0}) ->
    Groups = Manager#state_manager.groups,
    Tree = Manager#state_manager.tree,
    case groups_manager:get_closest_dcid(BKey, Groups, MyId, Tree) of
        {ok, MyId} ->
            case ets:lookup(KeyPrepared, BKey) of
                [] ->
                    {ok, {Value, _}} = ?BACKEND_CONNECTOR:read(Connector, {BKey, Clock}),
                    gen_fsm:send_event(Fsm, {new_value, BKey, Value}),
                    {noreply, S0};
                [{BKey, Orddict}] ->
                    case compute_wait_txs(Orddict, Clock, 0, []) of
                        {0, []} ->
                            {ok, {Value, _}} = ?BACKEND_CONNECTOR:read(Connector, {BKey, Clock}),
                            gen_fsm:send_event(Fsm, {new_value, BKey, Value}),
                            {noreply, S0};
                        {Length, List} ->
                            ReadId1 = ReadId0 + 1,
                            true = ets:insert(PendingCounter, {ReadId1, {Length, BKey, Clock, Fsm}}),
                            lists:foreach(fun(TxId) ->
                                            true = ets:insert(PendingReads, {TxId, ReadId1})
                                          end, List),
                            {noreply, S0#state{read_id=ReadId1}}
                    end
            end;
        {ok, Id} ->
            %Remote read
            PhysicalClock = saturn_utilities:now_microsec(),
            TimeStamp = max(Clock, max(PhysicalClock, MaxTS0+1)),
            Label = create_label(remote_read, BKey, TimeStamp, {Partition, node()}, MyId, #payload_remote{to=Id, client=Fsm, type_call=tx, version=Clock}),
            saturn_leaf_producer:new_label(MyId, Label, Partition, false),
            {noreply, S0#state{max_ts=TimeStamp, last_label=Label}};
        {error, Reason} ->
            lager:error("BKey ~p ~p in the dictionary",  [BKey, Reason]),
            gen_fsm:send_event(Fsm, {error, Reason}), 
            {noreply, S0}
    end;

handle_command({fsm_idle, Fsm}, _From, S0=#state{read_fsms=ReadFsms0, pending_readtxs=PendingReadTxs0, max_ts=MaxTS0}) ->
    case ets_queue:out(PendingReadTxs0) of
        {empty, _} ->
            ReadFsms1 = queue:in(Fsm, ReadFsms0),
            {noreply, S0#state{read_fsms=ReadFsms1}};
        {{value, {BKeys, Clock, Client}}, PendingReadTxs1} ->
            PhysicalClock = saturn_utilities:now_microsec(),
            ReadTime = max(Clock, max(PhysicalClock, MaxTS0)),
            gen_fsm:send_event(Fsm, {new_tx, BKeys, ReadTime, Client}),
            {noreply, S0#state{max_ts=ReadTime, pending_readtxs=PendingReadTxs1}}
    end;

handle_command({write_fsm_idle, Fsm}, _From, S0=#state{write_fsms=WriteFsms0, pending_writetxs=PendingWriteTxs0, max_ts=MaxTS0, myid=MyId, partition=Partition}) ->
    case ets_queue:out(PendingWriteTxs0) of
        {empty, _} ->
            WriteFsms1 = queue:in(Fsm, WriteFsms0),
            {noreply, S0#state{write_fsms=WriteFsms1}};
        {{value, {BKeyValuePairs, Clock, Client}}, PendingWriteTxs1} ->
            PhysicalClock = saturn_utilities:now_microsec(),
            TimeStamp = max(Clock+1, max(PhysicalClock, MaxTS0+1)),
            gen_fsm:send_event(Fsm, {new_tx, BKeyValuePairs, TimeStamp, Client}),
            BKeys = [BKey || {BKey, _Value} <- BKeyValuePairs],
            Label = create_label(write_tx, BKeys, TimeStamp, {Partition, node()}, MyId, {}),
            saturn_leaf_producer:new_label(MyId, Label, Partition, true),
            {noreply, S0#state{max_ts=TimeStamp, pending_writetxs=PendingWriteTxs1}}
    end;

handle_command({async_txwrite, BKeyValuePairs, Clock, Client}, _From, S0=#state{write_fsms=WriteFsms0, pending_writetxs=PendingWriteTxs0, max_ts=MaxTS0, myid=MyId, partition=Partition}) ->
    case queue:out(WriteFsms0) of
        {{value, Idle}, WriteFsms1} ->
            PhysicalClock = saturn_utilities:now_microsec(),
            TimeStamp = max(Clock+1, max(PhysicalClock, MaxTS0+1)),
            gen_fsm:send_event(Idle, {new_tx, BKeyValuePairs, TimeStamp, Client}),
            BKeys = [BKey || {BKey, _Value} <- BKeyValuePairs],
            Label = create_label(write_tx, BKeys, TimeStamp, {Partition, node()}, MyId, {}),
            saturn_leaf_producer:new_label(MyId, Label, Partition, true),
            {noreply, S0#state{max_ts=TimeStamp, write_fsms=WriteFsms1}};
        {empty, WriteFsms0} ->
            PendingWriteTxs1 = ets_queue:in({BKeyValuePairs, Clock, Client}, PendingWriteTxs0),
            {noreply, S0#state{pending_writetxs=PendingWriteTxs1}}
    end;

handle_command({prepare, TxId, Pairs, TimeStamp, Fsm}, _From, S0=#state{prepared_tx=PreparedTx, key_prepared=KeyPrepared, myid=MyId, manager=Manager, partition=Partition}) ->
    true = ets:insert(PreparedTx, {TxId, {Pairs, TimeStamp}}),
    Ignored = lists:foldl(fun({BKey, Value}, {Bool, Remote}) ->
                            case groups_manager:do_replicate(BKey, Manager#state_manager.groups, MyId) of
                                true ->
                                    case ets:lookup(KeyPrepared, BKey) of
                                        [{BKey, Orddict0}] ->
                                            Orddict1 = orddict:append(TimeStamp, {TxId, Value}, Orddict0);
                                        [] ->
                                            Orddict1 = orddict:append(TimeStamp, {TxId, Value}, orddict:new())
                                    end,
                                    true = ets:insert(KeyPrepared, {BKey, Orddict1}),
                                    {false, Remote};
                                false ->
                                    {Bool, [{BKey, Value}|Remote]};
                                {error, _Reason1} ->
                                    lager:error("BKey ~p not in the dictionary", [BKey]),
                                    {Bool, [{BKey, Value}|Remote]}
                            end
                         end, {true, []}, Pairs),
    gen_fsm:send_event(Fsm, {prepared, Ignored, {Partition, node()}}),
    {noreply, S0}; 

handle_command({remote_write_tx, Node, BKeys, Clock}, _From, S0=#state{remote_fsms=RemoteFsms0, pending_remotetxs=PendingRemoteTxs0}) ->
    case queue:out(RemoteFsms0) of
        {{value, Idle}, RemoteFsms1} ->
            gen_fsm:send_event(Idle, {new_tx, Node, BKeys, Clock}),
            {noreply, S0#state{remote_fsms=RemoteFsms1}};
        {empty, RemoteFsms0} ->
            PendingRemoteTxs1 = ets_queue:in({Node, BKeys, Clock}, PendingRemoteTxs0),
            {noreply, S0#state{pending_remotetxs=PendingRemoteTxs1}}
    end;

handle_command({remote_fsm_idle, Fsm}, _From, S0=#state{remote_fsms=RemoteFsms0, pending_remotetxs=PendingRemoteTxs0}) ->
    case ets_queue:out(PendingRemoteTxs0) of
        {empty, _} ->
            RemoteFsms1 = queue:in(Fsm, RemoteFsms0),
            {noreply, S0#state{remote_fsms=RemoteFsms1}};
        {{value, {Node, BKeys, Clock}}, PendingRemoteTxs1} ->
            gen_fsm:send_event(Fsm, {new_tx, Node, BKeys, Clock}),
            {noreply, S0#state{pending_remotetxs=PendingRemoteTxs1}}
    end;

handle_command({remote_prepare, TxId, Number, TimeStamp, Fsm}, _From, S0=#state{data=Data, remote=Remote0, key_prepared=KeyPrepared, prepared_tx=PreparedTx, partition=Partition}) ->
    %lager:info("Remote prepare (~p) for TxId: ~p, Number: ~p and TimeStamp: ~p", [Partition, TxId, Number, TimeStamp]),
    case ets:lookup(Data, TxId) of
        [] ->
            Remote1 = dict:store(TxId, {Number, Fsm}, Remote0),
            {noreply, S0#state{remote=Remote1}};
        List ->
            %lager:info("I (~p) already have the data ~p", [Partition, List]),
            case length(List) of
                Number ->
                    Remote1 = do_remote_prepare(TxId, TimeStamp, List, Data, Remote0, KeyPrepared, PreparedTx),
                    gen_fsm:send_event(Fsm, {prepared, {Partition, node()}}),
                    {noreply, S0#state{remote=Remote1}};
                Other ->
                    Remote1 = dict:store(TxId, {Number-Other, Fsm}, Remote0),
                    {noreply, S0#state{remote=Remote1}}
            end
    end;

handle_command({propagate_remote, TxId, Pairs}, _From, S0=#state{myid=MyId,
                                                                 manager=Manager,
                                                                 receivers=Receivers}) ->
    lists:foreach(fun({BKey, Value}) ->
                    propagate(TxId, BKey, Value, Manager#state_manager.groups, MyId, Receivers)
                  end, Pairs),
    {noreply, S0};

handle_command({commit, TxId, Remote}, _From, S0=#state{prepared_tx=PreparedTx,
                                                key_prepared=KeyPrepared,
                                                myid=MyId,
                                                manager=Manager,
                                                receivers=Receivers,
                                                pending_reads=PendingReads,
                                                pending_counter=PendingCounter,
                                                connector=Connector0}) ->
    [{TxId, {Pairs, TimeStamp}}] = ets:lookup(PreparedTx, TxId),
    Connector1 = lists:foldl(fun({BKey, Value}, Acc0) ->
                                {ok, Acc1} = ?BACKEND_CONNECTOR:update(Acc0, {BKey, Value, TimeStamp}),
                                case Remote of
                                    false ->
                                        propagate(TxId, BKey, Value, Manager#state_manager.groups, MyId, Receivers);
                                    true ->
                                        noop
                                end,
                                [{BKey, Orddict0}] = ets:lookup(KeyPrepared, BKey),
                                true = ets:insert(KeyPrepared, {BKey, clean_key_prepared(Orddict0, TimeStamp, TxId, [])}),
                                Acc1
                            end, Connector0, Pairs),
    case ets:lookup(PendingReads, TxId) of
        [] ->
            noop;
        Pendings ->
            lists:foreach(fun({_, Pending}) ->
                            case ets:lookup(PendingCounter, Pending) of
                                [{Pending, {1, BKey, Version, Fsm}}] ->
                                    {ok, {Value, _}} = ?BACKEND_CONNECTOR:read(Connector1, {BKey, Version}),
                                    gen_fsm:send_event(Fsm, {new_value, BKey, Value}),
                                    true = ets:delete(PendingCounter, Pending);
                                [{Pending, {Counter, BKey, Version, Fsm}}] ->
                                    true = ets:insert(PendingCounter, {Pending, {Counter-1, BKey, Version, Fsm}})
                            end
                         end, Pendings),
            true = ets:delete(PendingReads, TxId)
    end,
    true = ets:delete(PreparedTx, TxId),
    {noreply, S0#state{connector=Connector1}}; 

handle_command({update, BKey, Value, Clock}, _From, S0) ->
    {{ok, TimeStamp}, S1} = do_update(BKey, Value, Clock, S0),
    {reply, {ok, TimeStamp}, S1};

handle_command({async_update, BKey, Value, Clock, Client}, _From, S0) ->
    {{ok, TimeStamp}, S1} = do_update(BKey, Value, Clock, S0),
    gen_server:reply(Client, {ok, TimeStamp}),
    {noreply, S1};

handle_command({propagate, BKey, TimeStamp, Sender, Node}, _From, S0=#state{connector=Connector0, myid=MyId, remote=Remote0, data=Data, staleness=Staleness}) ->
    Id = {TimeStamp, Node},
    %lager:info("I have received the metadata: TxId ~p, BKey ~p", [Id, BKey]),
    case ets:lookup(Data, Id) of
        [] ->
            Remote1 = dict:store(Id, {single, Sender}, Remote0),
            {noreply, S0#state{remote=Remote1}};
        [{Id, {BKey, Value}}] ->
            Staleness1 = ?STALENESS:add_update(Staleness, Sender, TimeStamp),
            {ok, Connector1} = ?BACKEND_CONNECTOR:update(Connector0, {BKey, Value, TimeStamp}),
            true = ets:delete(Data, Id),
            %lager:info("Sending completed"),
            saturn_leaf_converger:handle(MyId, {completed, TimeStamp}),
            {noreply, S0#state{connector=Connector1, staleness=Staleness1}}
    end;
    
handle_command({remote_read, Label}, _From, S0=#state{max_ts=MaxTS0, myid=MyId, partition=Partition, connector=Connector, staleness=Staleness}) ->
    Sender = Label#label.sender,
    TimeStamp = Label#label.timestamp,
    Staleness1 = ?STALENESS:add_remote(Staleness, Sender, TimeStamp),
    BKeyToRead = Label#label.bkey,
    Payload = Label#label.payload,
    Version = Payload#payload_remote.version,
    {ok, {Value, _Clock}} = ?BACKEND_CONNECTOR:read(Connector, {BKeyToRead, Version}),
    PhysicalClock = saturn_utilities:now_microsec(),
    NewTimeStamp = max(PhysicalClock, MaxTS0+1),
    Client = Payload#payload_remote.client,
    Type = Payload#payload_remote.type_call,
    NewLabel = create_label(remote_reply, {routing, routing}, NewTimeStamp, {Partition, node()}, MyId, #payload_reply{value=Value, to=Sender, client=Client, type_call=Type, bkey=BKeyToRead}),
    saturn_leaf_producer:new_label(MyId, NewLabel, Partition, false),
    {noreply, S0#state{max_ts=NewTimeStamp, last_label=NewLabel, staleness=Staleness1}};

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
    case Type of
        tx ->
            Version = Clock;
        _ ->
            Version = latest
    end,
    case groups_manager:get_closest_dcid(BKey, Groups, MyId, Tree) of
        {ok, MyId} ->    
            ?BACKEND_CONNECTOR:read(Connector, {BKey, Version});
        {ok, Id} ->
            %Remote read
            PhysicalClock = saturn_utilities:now_microsec(),
            TimeStamp = max(Clock, max(PhysicalClock, MaxTS0)),
            Label = create_label(remote_read, BKey, TimeStamp, {Partition, node()}, MyId, #payload_remote{to=Id, client=From, type_call=Type, version=Version}),
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
    UId = {Partition, node()},
    Label = create_label(update, BKey, TimeStamp, UId, MyId, {}),
    saturn_leaf_producer:new_label(MyId, Label, Partition, true),
    case groups_manager:get_datanodes_ids(BKey, Manager#state_manager.groups, MyId) of
        {ok, Group} ->
            lists:foreach(fun(Id) ->
                            Receiver = dict:fetch(Id, Receivers),
                            saturn_data_receiver:data(Receiver, {TimeStamp, UId}, BKey, Value)
                          end, Group);
        {error, Reason2} ->
            lager:error("No replication group for bkey: ~p (~p)", [BKey, Reason2])
    end,
    {{ok, TimeStamp}, S1#state{max_ts=TimeStamp, last_label=Label}}.

compute_wait_txs([], _Version, Length, WaitList) ->
    {Length, WaitList};

compute_wait_txs([Next|Rest], Version, Length, WaitList) ->
    {TimeStamp, Txs} = Next,
    case TimeStamp > Version of
        true ->
            {Length, WaitList};
        false ->
            {WaitList1, Sum} = lists:foldl(fun({TxId, _Value}, {Acc0, Acc1}) ->
                                            {[TxId|Acc0], Acc1+1}
                                           end, {WaitList, 0}, Txs),
            compute_wait_txs(Rest, Version, Length+Sum, WaitList1)
    end.

do_remote_prepare(TxId, TimeStamp, List, Data, Remote0, KeyPrepared, PreparedTx) ->
    true = ets:delete(Data, TxId),
    Pairs = lists:foldl(fun({_TxId, {BKey, Value}}, Acc) ->
                            case ets:lookup(KeyPrepared, BKey) of
                                [{BKey, Orddict0}] ->
                                    Orddict1 = orddict:append(TimeStamp, {TxId, Value}, Orddict0);
                                [] ->
                                    Orddict1 = orddict:append(TimeStamp, {TxId, Value}, orddict:new())
                            end,
                            true = ets:insert(KeyPrepared, {BKey, Orddict1}),
                            [{BKey, Value}|Acc]
                        end, [], List),
    true = ets:insert(PreparedTx, {TxId, {Pairs, TimeStamp}}),
    dict:erase(TxId, Remote0).

remove_txid_from_keyprepared([], _TxId, _NewList) ->
    false;

remove_txid_from_keyprepared([{TxId, _Value}|Rest], TxId, NewList) ->
    NewList1 = lists:foldl(fun(Elem, Acc) ->
                            [Elem|Acc]
                           end, NewList, Rest),
    {true, NewList1};

remove_txid_from_keyprepared([Next|Rest], TxId, NewList) ->
    remove_txid_from_keyprepared(Rest, TxId, [Next|NewList]).

clean_key_prepared([], _TimeStamp, TxId, NewOrddict) ->
    lager:error("Txid ~p not found in list of prepared keys ~p", [TxId, NewOrddict]),
    lists:reverse(NewOrddict);

clean_key_prepared([{TimeStamp, List0}|Rest], TimeStamp, TxId, NewOrddict) ->
    case remove_txid_from_keyprepared(List0, TxId, []) of
        {true, []} ->
            NewOrddict1 = lists:foldl(fun(Elem, Acc) ->
                                        [Elem|Acc]
                                      end, NewOrddict, Rest),
            lists:reverse(NewOrddict1);
        {true, List1} ->
            NewOrddict1 = lists:foldl(fun(Elem, Acc) ->
                                        [Elem|Acc]
                                      end, [{TimeStamp, List1}|NewOrddict], Rest),
            lists:reverse(NewOrddict1);
        false ->
            lager:error("Txid ~p not found in list of prepared keys ~p for timestamp ~p", [TxId, List0, TimeStamp]),
            NewOrddict1 = lists:foldl(fun(Elem, Acc) ->
                                        [Elem|Acc]
                                      end, [{TimeStamp, List0}|NewOrddict], Rest),
            lists:reverse(NewOrddict1)
    end;

clean_key_prepared([Next|Rest], TimeStamp, TxId, NewOrddict) ->
    clean_key_prepared(Rest, TimeStamp, TxId, [Next|NewOrddict]).

propagate(TxId, BKey, Value, Groups, MyId, Receivers) ->
    case groups_manager:get_datanodes_ids(BKey, Groups, MyId) of
        {ok, Group} ->
            lists:foreach(fun(Id) ->
                            Receiver = dict:fetch(Id, Receivers),
                            saturn_data_receiver:data(Receiver, TxId, BKey, Value)
                          end, Group);
        {error, Reason2} ->
            lager:error("No replication group for bkey: ~p (~p)", [BKey, Reason2])
    end.
