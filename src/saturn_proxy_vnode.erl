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

-export([init_vv/4,
         read/3,
         update/4,
         async_read/4,
         async_update/5,
         propagate/5,
         remote_read/6,
         remote_reply/6,
         send_heartbeat/1,
         heartbeat/3,
         compute_times/1,
         set_receivers/2,
         set_tree/4,
         set_groups/2,
         clean_state/1,
         collect_stats/3,
         new_gst/2,
         init_update/2,
         init_list/2,
         check_ready/1]).

-record(state, {partition,
                vv :: dict(),
                vv_remote :: dict(),
                gst,
                last_physical,
                connector,
                receivers,
                manager,
                staleness,
                parent,
                children,
                remotes,
                myid}).

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init_vv(Node, Entries, Partitions, MyId) ->
    riak_core_vnode_master:sync_command(Node,
                                        {init_vv, Entries, Partitions, MyId},
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

update(Node, BKey, Value, Clock) ->
    riak_core_vnode_master:sync_command(Node,
                                        {update, BKey, Value, Clock},
                                        ?PROXY_MASTER).

init_update(Node, BKey) ->
    riak_core_vnode_master:sync_command(Node,
                                        {init_update, BKey},
                                        ?PROXY_MASTER).

init_list(Node, List) ->
    riak_core_vnode_master:sync_command(Node,
                                        {init_list, List},
                                        ?PROXY_MASTER).

async_update(Node, BKey, Value, Clock, Client) ->
    riak_core_vnode_master:command(Node,
                                   {async_update, BKey, Value, Clock, Client},
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).

propagate(Node, BKey, Value, TimeStamp, Sender) ->
    riak_core_vnode_master:command(Node,
                                   {propagate, BKey, Value, TimeStamp, Sender},
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).

remote_read(Node, BKey, Sender, Clock, Client, Type) ->
    riak_core_vnode_master:command(Node,
                                   {remote_read, BKey, Sender, Clock, Client, Type},
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).

remote_reply(Node, Sender, Value, Client, Clock, Type) ->
    riak_core_vnode_master:command(Node,
                                   {remote_reply, Sender, Value, Client, Clock, Type},
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).

send_heartbeat(Node) ->
    riak_core_vnode_master:command(Node,
                                   send_heartbeat,
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).

heartbeat(Node, Clock, Sender) ->
    riak_core_vnode_master:command(Node,
                                   {heartbeat, Clock, Sender},
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).

compute_times(Node) ->
    riak_core_vnode_master:command(Node,
                                   compute_times,
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).

new_gst(Node, GST) ->
    riak_core_vnode_master:command(Node,
                                   {new_gst, GST},
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).
       

set_receivers(Node, Receivers) ->
    riak_core_vnode_master:sync_command(Node,
                                        {set_receivers, Receivers},
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

collect_stats(Node, From, Type) ->
    riak_core_vnode_master:sync_command(Node,
                                        {collect_stats, From, Type},
                                        ?PROXY_MASTER).

init([Partition]) ->
    Manager = groups_manager:init_state(integer_to_list(Partition)),
    Connector = ?BACKEND_CONNECTOR:connect([Partition]),
    %Name = list_to_atom(integer_to_list(Partition) ++ atom_to_list(gentle_rain_pops)),
    %POps = ets:new(Name, [ordered_set, named_table, private]),
    Name2 = list_to_atom(integer_to_list(Partition) ++ atom_to_list(staleness)),
    Staleness = ?STALENESS:init(Name2),
    lager:info("Vnode init: ~p", [Partition]),
    {ok, #state{partition=Partition,
                vv=dict:new(),
                vv_remote=dict:new(),
                gst=0,
                manager=Manager,
                last_physical=0,
                connector=Connector,
                staleness=Staleness,
                remotes=dict:new()
               }}.

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

handle_command({init_vv, Entries, _Partitions, MyId}, _From, S0=#state{vv=VV0, vv_remote=VVRemote0, partition=Partition}) ->
    FilteredEntries = lists:delete(MyId, Entries),
    VV1 = lists:foldl(fun(Entry, Acc) ->
                        dict:store(Entry, 0, Acc)
                     end, VV0, Entries),
    VVRemote1 = lists:foldl(fun(Entry, Acc) ->
                        dict:store(Entry, 0, Acc)
                     end, VVRemote0, FilteredEntries),
    Pendings1 = lists:foldl(fun(Entry, Acc) ->
                                Name = list_to_atom(integer_to_list(Partition) ++ integer_to_list(Entry) ++  atom_to_list(gentlerain_pops)),
                                dict:store(Entry, ets_queue:new(Name), Acc)
                            end, dict:new(), FilteredEntries),
    {reply, ok, S0#state{vv=VV1, vv_remote=VVRemote1, myid=MyId, remotes=Pendings1}};

handle_command({set_tree, Paths, Tree, NLeaves}, _From, S0=#state{manager=Manager, myid=MyId, partition=Partition, remotes=Pendings0}) ->
    Entries = lists:seq(0, NLeaves-1),
    FilteredEntries = lists:delete(MyId, Entries),
    VV1 = lists:foldl(fun(Entry, Acc) ->
                        dict:store(Entry, 0, Acc)
                     end, dict:new(), Entries),
    VVRemote1 = lists:foldl(fun(Entry, Acc) ->
                        dict:store(Entry, 0, Acc)
                     end, dict:new(), FilteredEntries),
     lists:foreach(fun({_, Queue}) ->
                    ok = ets_queue:delete(Queue)
                  end, dict:to_list(Pendings0)),
    Pendings1 = lists:foldl(fun(Entry, Acc) ->
                                Name = list_to_atom(integer_to_list(Partition) ++ integer_to_list(Entry) ++  atom_to_list(gentlerain_pops)),
                                dict:store(Entry, ets_queue:new(Name), Acc)
                            end, dict:new(), FilteredEntries),
    {reply, ok, S0#state{manager=Manager#state_manager{tree=Tree, paths=Paths, nleaves=NLeaves}, vv=VV1, vv_remote=VVRemote1, remotes=Pendings1}};

handle_command({set_groups, Groups}, _From, S0=#state{manager=Manager}) ->
    Table = Manager#state_manager.groups,
    ok = groups_manager:set_groups(Table, Groups),
    {reply, ok, S0};

handle_command({init_update, BKey}, _From, S0=#state{connector=Connector0}) ->
    {ok, Connector1} = ?BACKEND_CONNECTOR:init_update(Connector0, BKey),
    {reply, ok, S0#state{connector=Connector1}};

handle_command({init_list, List}, _From, S0=#state{connector=Connector0}) ->
    Connector1 = lists:foldl(fun(BKey, Acc) ->
                                {ok, Acc1} = ?BACKEND_CONNECTOR:init_update(Acc, BKey),
                                Acc1
                             end, Connector0, List),
    {reply, ok, S0#state{connector=Connector1}};

handle_command({collect_stats, From, Type}, _Sender, S0=#state{staleness=Staleness}) ->
    {reply, {ok, ?STALENESS:compute_raw(Staleness, From, Type)}, S0};

handle_command(clean_state, _Sender, S0=#state{connector=Connector0, partition=Partition, vv=VV, vv_remote=VVRemote, staleness=Staleness, remotes=Pendings0}) ->
    Connector1 = ?BACKEND_CONNECTOR:clean(Connector0, Partition),
    Name2 = list_to_atom(integer_to_list(Partition) ++ atom_to_list(staleness)),
    Staleness1 = ?STALENESS:clean(Staleness, Name2),
    Pendings1 = lists:foldl(fun({Entry, Queue}, Acc) ->
                                dict:store(Entry, ets_queue:clean(Queue), Acc)
                            end, dict:new(), dict:to_list(Pendings0)),
    {reply, ok, S0#state{vv=clean_vector(VV),
                         vv_remote=clean_vector(VVRemote),
                         gst=0,
                         last_physical=0,
                         staleness=Staleness1,
                         remotes=Pendings1,
                         connector=Connector1}};

handle_command({read, BKey, Clock}, From, S0) ->
    case do_read(sync, BKey, Clock, From, S0) of
        {ok, Result, S1} ->
            {reply, {ok, Result}, S1};
        {remote, S1} ->
            {noreply, S1};
        {error, Reason} ->
            {reply, {error, Reason}, S0}
    end;

handle_command({async_read, BKey, Clock, Client}, _From, S0) ->
    case do_read(async, BKey, Clock, Client, S0) of
        {ok, Result, S1} ->
            gen_server:reply(Client, {ok, Result}),
            {noreply, S1};
        {remote, S1} ->
            %gen_server:reply(Client, {ok, {bottom, 0, 0}}),
            {noreply, S1};
        {error, Reason} ->
            gen_server:reply(Client, {error, Reason}),
            {noreply, S0}
    end;

handle_command({update, BKey, Value, Clock}, _From, S0) ->
    {{ok, TimeStamp}, S1} =  do_update(BKey, Value, Clock, S0),
    {reply, {ok, TimeStamp}, S1};

handle_command({async_update, BKey, Value, Clock, Client}, _From, S0) ->
    {{ok, TimeStamp}, S1} =  do_update(BKey, Value, Clock, S0),
    gen_server:reply(Client, {ok, TimeStamp}),
    {noreply, S1};

handle_command({set_receivers, Receivers}, _From, S0) ->
    {reply, ok, S0#state{receivers=Receivers}};

handle_command({propagate, BKey, Value, TimeStamp, Sender}, _From, S0=#state{connector=Connector0, gst=GST, vv=VV0, receivers=Receivers, staleness=Staleness, myid=MyId}) ->
    %lager:info("Received a remote update. Key ~p, Value ~p, TS ~p, Sender ~p",[BKey, Value, TimeStamp, Sender]),
    VV1 = dict:store(Sender, TimeStamp, VV0),
    %lager:info("GST: ~p, Timestamp: ~p", [GST, TimeStamp]),
    {Connector1, Staleness1} = handle_operation(update, {BKey, Value, TimeStamp, Sender}, Connector0, GST, Receivers, Staleness, MyId),
    {noreply, S0#state{vv=VV1, connector=Connector1, staleness=Staleness1}};

handle_command({remote_read, BKey, Sender, Clock, Client, Type}, _From, S0=#state{connector=Connector0, gst=GST, receivers=Receivers, staleness=Staleness, remotes=Pendings0, myid=MyId}) ->
    %lager:info("Received a remote read. Key ~p, Sender ~p, TS ~p, Client ~p",[BKey, Sender, Clock, Client]),
    %lager:info("GST: ~p, Timestamp: ~p", [GST, Clock]),
    case GST >= Clock of
        true ->
            {Connector1, Staleness1} = handle_operation(remote_read, {BKey, Sender, Client, Type, Clock}, Connector0, GST, Receivers, Staleness, MyId),
            {noreply, S0#state{connector=Connector1, staleness=Staleness1}};
        false ->
            Queue0 = dict:fetch(Sender, Pendings0),
            Queue1 = ets_queue:in({Clock, Sender, {remote_read, {BKey, Sender, Client, Type, Clock}}}, Queue0),
            Pendings1 = dict:store(Sender, Queue1, Pendings0),
            {noreply, S0#state{remotes=Pendings1}}
    end;

handle_command({remote_reply, Sender, Value, Client, Clock, Type}, _From, S0=#state{connector=Connector0, gst=GST, receivers=Receivers, staleness=Staleness, remotes=Pendings0, myid=MyId}) ->
    %lager:info("Received a remote reply. Value ~p, Client ~p, Clock ~p",[Value, Client, Clock]),
    %lager:info("GST: ~p, Timestamp: ~p", [GST, Clock]),
    case GST >= Clock of
        true ->
            {Connector1, Staleness1} = handle_operation(remote_reply, {Value, Client, Clock, Type}, Connector0, GST, Receivers, Staleness, MyId),
            {noreply, S0#state{connector=Connector1, staleness=Staleness1}};
        false ->
            Queue0 = dict:fetch(Sender, Pendings0),
            Queue1 = ets_queue:in({Clock, Client, {remote_reply, {Value, Client, Clock, Type}}}, Queue0),
            Pendings1 = dict:store(Sender, Queue1, Pendings0),
            {noreply, S0#state{remotes=Pendings1}}
    end;

handle_command(send_heartbeat, _From, S0=#state{partition=Partition, vv=VV0, vv_remote=VVRemote0, myid=MyId, receivers=Receivers}) ->
    PhysicalClock0 = saturn_utilities:now_microsec(),
    Max = max(dict:fetch(MyId, VV0), PhysicalClock0),
    VVRemote1 = lists:foldl(fun(Id, Acc) ->
                                Clock = dict:fetch(Id, Acc),
                                case ((Clock + ?HEARTBEAT_FREQ*1000) < Max) of
                                    true ->
                                        Receiver = dict:fetch(Id, Receivers),
                                        saturn_leaf_converger:heartbeat(Receiver, Partition, Max, MyId),
                                        dict:store(Id, Max, Acc);
                                    false ->
                                        Acc
                                end
                            end, VVRemote0, dict:fetch_keys(VVRemote0)),
    VV1 = dict:store(MyId, Max, VV0),
    riak_core_vnode:send_command_after(?HEARTBEAT_FREQ, send_heartbeat),
    {noreply, S0#state{vv_remote=VVRemote1, vv=VV1}};

handle_command({heartbeat, Clock, Id}, _From, S0=#state{vv=VV0}) ->
    %lager:info("Heartbeat received: ~p from ~p", [Clock, Id]),
    VV1 = dict:store(Id, Clock, VV0),
    {noreply, S0#state{vv=VV1}};

handle_command(compute_times, _From, S0=#state{vv=VV, partition=Partition}) ->
    LST = lists:foldl(fun(Id, Min) ->
                        min(dict:fetch(Id, VV), Min)
                     end, infinity, dict:fetch_keys(VV)),
    saturn_client_receiver:new_clock(node(), Partition, LST),
    riak_core_vnode:send_command_after(?TIMES_FREQ, compute_times),
    {noreply, S0};

handle_command({new_gst, GST}, _From, S0=#state{receivers=Receivers, staleness=Staleness, connector=Connector0, remotes=Pendings0, myid=MyId}) ->
    %lager:info("New gst ~p", [GST]),
    Staleness0 = ?STALENESS:add_gst(Staleness, GST),
    {Pendings1, Connector1, Staleness1} = flush_pending_operations(Pendings0, GST, Connector0, Receivers, Staleness0, MyId),  
    {noreply, S0#state{gst=GST, staleness=Staleness1, remotes=Pendings1, connector=Connector1}};

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

flush_pending_operations(PendingsBase, GST, Connector, Receivers, Staleness, MyId) ->
    lists:foldl(fun({Entry, Queue}, {Pendings0, Connector0, Staleness0}) ->
                    {Queue1, Connector1, Staleness1} = flush_pending_operations_internal(ets_queue:peek(Queue), Queue, GST, Connector0, Receivers, Staleness0, MyId),
                    Pendings1 = dict:store(Entry, Queue1, Pendings0),
                    {Pendings1, Connector1, Staleness1}
                end, {dict:new(), Connector, Staleness}, dict:to_list(PendingsBase)).
   
flush_pending_operations_internal(empty, Queue, _GST, Connector0, _Receivers, Staleness, _MyId) ->
    {Queue, Connector0, Staleness};

flush_pending_operations_internal({value, Next}, Queue, GST, Connector0, Receivers, Staleness, MyId) ->
    {TimeStamp, _Sender, {Op, Payload}} = Next,
    case (TimeStamp =< GST) of
        true ->
            {Connector1, Staleness1} = handle_operation(Op, Payload, Connector0, GST, Receivers, Staleness, MyId),
            {{value, _}, Queue1} = ets_queue:out(Queue),
            flush_pending_operations_internal(ets_queue:peek(Queue1), Queue1, GST, Connector1, Receivers, Staleness1, MyId);
        false ->
            {Queue, Connector0, Staleness}
    end.

handle_operation(Type, Payload, Connector0, GST, Receivers, Staleness, MyId) ->
    case Type of
        update ->
            {BKey, Value, TimeStamp, Sender} = Payload,
            Staleness1 = ?STALENESS:add_update(Staleness, Sender, TimeStamp),
            {ok, Connector1} = ?BACKEND_CONNECTOR:update(Connector0, {BKey, Value, TimeStamp, Sender}),
            {Connector1, Staleness1};
        remote_read ->
            {BKey, Sender, Client, Call, TimeStamp} = Payload,
            Staleness1 = ?STALENESS:add_remote(Staleness, Sender, TimeStamp),
            {ok, {StoredValue, StoredTimeStamp}} = ?BACKEND_CONNECTOR:remote_read(Connector0, {BKey, latest}),
            Receiver = dict:fetch(Sender, Receivers),
            saturn_leaf_converger:remote_reply(Receiver, MyId, BKey, StoredValue, Client, StoredTimeStamp, Call),
            {Connector0, Staleness1};
        remote_reply ->
            {Value, Client, Clock, Call} = Payload,
            case Call of
                sync ->
                    %noop;
                    riak_core_vnode:reply(Client, {ok, {Value, Clock, GST}});
                async ->
                    %noop
                    gen_server:reply(Client, {ok, {Value, Clock, GST}})
            end,
            {Connector0, Staleness};
        _ ->
            lager:error("Unhandled pending operation of type: ~p with payload ~p", [Type, Payload]),
            {Connector0, Staleness}
    end.

do_read(Type, BKey, {ClientGST, ClientClock}, From, S0=#state{last_physical=LastPhysical, myid=MyId, connector=Connector0, gst=GST0, receivers=Receivers, manager=Manager, staleness=Staleness, remotes=Pendings0, vv=VV0}) ->
    GST1 = max(GST0, ClientGST),
    Staleness0 = ?STALENESS:add_gst(Staleness, GST1),
    {Pendings1, Connector1, Staleness1} = flush_pending_operations(Pendings0, GST1, Connector0, Receivers, Staleness0, MyId),  
    Groups = Manager#state_manager.groups,
    Tree = Manager#state_manager.tree,
    case groups_manager:get_closest_dcid(BKey, Groups, MyId, Tree) of
        {ok, MyId} ->
            {ok, {Value, Ts}} = ?BACKEND_CONNECTOR:read(Connector1, {BKey, GST1, MyId}),
            {ok, {Value, Ts, GST1}, S0#state{gst=GST1, staleness=Staleness1, remotes=Pendings1, connector=Connector1}};
        {ok, Id} ->
            Clock = max(ClientGST, ClientClock),
            PhysicalClock0 = saturn_utilities:now_microsec(),
            PhysicalClock1 = max(PhysicalClock0, LastPhysical+1),
            Dif = Clock - PhysicalClock1,
            case Dif==0 of
                true ->
                    TimeStamp = PhysicalClock1 + 1;
                false ->
                    case Dif > 0 of
                        true ->
                            timer:sleep(trunc(Dif/1000)),
                            TimeStamp = Clock + 1;
                        false ->
                            TimeStamp = PhysicalClock1
                    end
            end,
            VV1 = dict:store(MyId, TimeStamp, VV0),
            %Remote read
            Receiver = dict:fetch(Id, Receivers),
            saturn_leaf_converger:remote_read(Receiver, BKey, MyId, TimeStamp, From, Type),
            {remote, S0#state{gst=GST1, staleness=Staleness1, remotes=Pendings1, last_physical=PhysicalClock1, connector=Connector1, vv=VV1}};
        {error, Reason} ->
            lager:error("BKey ~p ~p in the dictionary",  [BKey, Reason]),
            {error, Reason}
    end.

do_update(BKey, Value, Clock, S0=#state{last_physical=LastPhysical, myid=MyId, connector=Connector0, vv=VV0, vv_remote=VVRemote0, receivers=Receivers, manager=Manager}) ->
    PhysicalClock0 = saturn_utilities:now_microsec(),
    PhysicalClock1 = max(PhysicalClock0, LastPhysical+1),
    Dif = Clock - PhysicalClock1,
    case Dif==0 of
        true ->
            TimeStamp = PhysicalClock1 + 1;
        false ->
            case Dif > 0 of
                true ->
                    timer:sleep(trunc(Dif/1000)),
                    TimeStamp = Clock + 1;
                false ->
                    TimeStamp = PhysicalClock1
            end
    end,
    VV1 = dict:store(MyId, TimeStamp, VV0),
    S1 = case groups_manager:do_replicate(BKey, Manager#state_manager.groups, MyId) of
        true ->
            {ok, Connector1} = ?BACKEND_CONNECTOR:update(Connector0, {BKey, Value, TimeStamp, MyId}),
            S0#state{connector=Connector1};
        false ->
            S0;
        {error, Reason1} ->
            lager:error("BKey ~p ~p in the dictionary",  [BKey, Reason1]),
            S0
    end,
    case groups_manager:get_datanodes_ids(BKey, Manager#state_manager.groups, MyId) of
        {ok, Group} ->
            VVRemote1 = lists:foldl(fun(Id, Acc) ->
                                        Receiver = dict:fetch(Id, Receivers),
                                        saturn_leaf_converger:propagate(Receiver, BKey, Value, TimeStamp, MyId),
                                        dict:store(Id, TimeStamp, Acc)
                                    end, VVRemote0, Group);
        {error, Reason2} ->
            lager:error("No replication group for bkey: ~p (~p)", [BKey, Reason2]),
            VVRemote1 = VVRemote0
    end,
    {{ok, TimeStamp}, S1#state{last_physical=PhysicalClock1, vv=VV1, vv_remote=VVRemote1}}.

clean_vector(Vector) ->
    lists:foldl(fun(Entry, Acc) ->
                    dict:store(Entry, 0, Acc)
                end, dict:new(), dict:fetch_keys(Vector)).
