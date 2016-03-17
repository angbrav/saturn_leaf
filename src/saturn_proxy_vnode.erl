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
         propagate/5,
         remote_read/5,
         remote_reply/4,
         send_heartbeat/1,
         heartbeat/3,
         new_lst/3,
         compute_times/1,
         check_ready/1]).

-record(state, {partition,
                vv :: dict(),
                vv_remote :: dict(),
                vv_lst :: dict(),
                gst,
                last_physical,
                connector,
                pops,
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

update(Node, BKey, Value, Clock) ->
    riak_core_vnode_master:sync_command(Node,
                                        {update, BKey, Value, Clock},
                                        ?PROXY_MASTER).

propagate(Node, BKey, Value, TimeStamp, Sender) ->
    riak_core_vnode_master:command(Node,
                                   {propagate, BKey, Value, TimeStamp, Sender},
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).

remote_read(Node, BKey, Sender, Clock, Client) ->
    riak_core_vnode_master:command(Node,
                                   {remote_read, BKey, Sender, Clock, Client},
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).

remote_reply(Node, Value, Client, Clock) ->
    riak_core_vnode_master:command(Node,
                                   {remote_reply, Value, Client, Clock},
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

new_lst(Node, Partition, Clock) ->
    riak_core_vnode_master:command(Node,
                                   {new_lst, Partition, Clock},
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).

compute_times(Node) ->
    riak_core_vnode_master:command(Node,
                                   compute_times,
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).

init([Partition]) ->
    lager:info("Vnode init: ~p", [Partition]),
    Connector = ?BACKEND_CONNECTOR:connect([Partition]),
    {ok, #state{partition=Partition,
                vv=dict:new(),
                vv_remote=dict:new(),
                vv_lst=dict:new(),
                gst=0,
                last_physical=0,
                pops=orddict:new(),
                connector=Connector
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

handle_command({check_myid_ready}, _Sender, S0) ->
    S1 = check_myid(S0),
    {reply, true, S1};

handle_command({check_tables_ready}, _Sender, SD0) ->
    {reply, true, SD0};

handle_command({init_vv, Entries, Partitions, MyId}, _From, S0=#state{vv=VV0, vv_lst=VV_LST0, vv_remote=VVRemote0}) ->
    FilteredEntries = lists:delete(MyId, Entries),
    VV1 = lists:foldl(fun(Entry, Acc) ->
                        dict:store(Entry, 0, Acc)
                     end, VV0, Entries),
    VVRemote1 = lists:foldl(fun(Entry, Acc) ->
                        dict:store(Entry, 0, Acc)
                     end, VVRemote0, FilteredEntries),
    VV_LST1 = lists:foldl(fun(Partition, Acc) ->
                            dict:store(Partition, 0, Acc)
                         end, VV_LST0, Partitions),
    groups_manager_serv:set_myid(MyId),
    {reply, ok, S0#state{vv=VV1, vv_lst=VV_LST1, vv_remote=VVRemote1, myid=MyId}};

handle_command({read, BKey, {ClientGST, ClientClock}}, From, S0=#state{myid=MyId, connector=Connector, gst=GST0}) ->
    GST1 = max(GST0, ClientGST),
    case groups_manager_serv:get_closest_dcid(BKey) of
        {ok, MyId} ->
            {ok, {Value, Ts}} = ?BACKEND_CONNECTOR:read(Connector, {BKey}),
            {reply, {ok, {Value, Ts, GST1}}, S0#state{gst=GST1}};
        {ok, Id} ->
            %Remote read
            saturn_leaf_converger:remote_read(Id, BKey, MyId, max(ClientGST, ClientClock), From),
            {noreply, S0#state{gst=GST1}};
        {error, Reason} ->
            lager:error("BKey ~p ~p in the dictionary",  [BKey, Reason]),
            {reply, {error, Reason}, S0}
    end;

handle_command({update, BKey, Value, Clock}, _From, S0=#state{last_physical=LastPhysical, myid=MyId, connector=Connector0, vv=VV0, vv_remote=VVRemote0}) ->
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
    S1 = case groups_manager_serv:do_replicate(BKey) of
        true ->
            {ok, Connector1} = ?BACKEND_CONNECTOR:update(Connector0, {BKey, Value, TimeStamp}),
            S0#state{connector=Connector1};
        false ->
            S0;
        {error, Reason1} ->
            lager:error("BKey ~p ~p in the dictionary",  [BKey, Reason1]),
            S0
    end,
    case groups_manager_serv:get_datanodes_ids(BKey) of
        {ok, Group} ->
            VVRemote1 = lists:foldl(fun(Id, Acc) ->
                                        saturn_leaf_converger:propagate(Id, BKey, Value, TimeStamp, MyId),
                                        dict:store(Id, TimeStamp, Acc)
                                    end, VVRemote0, Group);
        {error, Reason2} ->
            lager:error("No replication group for bkey: ~p (~p)", [BKey, Reason2]),
            VVRemote1 = VVRemote0
    end,
    {reply, {ok, TimeStamp}, S1#state{last_physical=PhysicalClock1, vv=VV1, vv_remote=VVRemote1}};

handle_command({propagate, BKey, Value, TimeStamp, Sender}, _From, S0=#state{connector=Connector0, gst=GST, vv=VV0, pops=PendingOps0}) ->
    %lager:info("Received a remote update. Key ~p, Value ~p, TS ~p, Sender ~p",[BKey, Value, TimeStamp, Sender]),
    VV1 = dict:store(Sender, TimeStamp, VV0),
    %lager:info("GST: ~p, Timestamp: ~p", [GST, TimeStamp]),
    case GST >= TimeStamp of
        true ->
            Connector1 = handle_operation(update, {BKey, Value, TimeStamp}, Connector0, GST),
            {noreply, S0#state{vv=VV1, connector=Connector1}};
        false ->
            PendingOps1 = orddict:append(TimeStamp, {update, {BKey, Value, TimeStamp}}, PendingOps0),
            {noreply, S0#state{pops=PendingOps1, vv=VV1}}
    end;

handle_command({remote_read, BKey, Sender, Clock, Client}, _From, S0=#state{pops=PendingOps0, connector=Connector0, gst=GST}) ->
    lager:info("Received a remote read. Key ~p, Sender ~p, TS ~p, Client ~p",[BKey, Sender, Clock, Client]),
    lager:info("GST: ~p, Timestamp: ~p", [GST, Clock]),
    case GST >= Clock of
        true ->
            Connector1 = handle_operation(remote_read, {BKey, Sender, Client}, Connector0, GST),
            {noreply, S0#state{connector=Connector1}};
        false ->
            PendingOps1 = orddict:append(Clock, {remote_read, {BKey, Sender, Client}}, PendingOps0),
            {noreply, S0#state{pops=PendingOps1}}
    end;

handle_command({remote_reply, Value, Client, Clock}, _From, S0=#state{pops=PendingOps0, connector=Connector0, gst=GST}) ->
    lager:info("Received a remote reply. Value ~p, Client ~p, Clock ~p",[Value, Client, Clock]),
    lager:info("GST: ~p, Timestamp: ~p", [GST, Clock]),
    case GST >= Clock of
        true ->
            Connector1 = handle_operation(remote_reply, {Value, Client, Clock}, Connector0, GST),
            {noreply, S0#state{connector=Connector1}};
        false ->
            PendingOps1 = orddict:append(Clock, {remote_reply, {Value, Client, Clock}}, PendingOps0),
            {noreply, S0#state{pops=PendingOps1}}
    end;

handle_command(send_heartbeat, _From, S0=#state{partition=Partition, vv=VV0, vv_remote=VVRemote0, myid=MyId}) ->
    PhysicalClock0 = saturn_utilities:now_microsec(),
    Max = max(dict:fetch(MyId, VV0), PhysicalClock0),
    VVRemote1 = lists:foldl(fun(Id, Acc) ->
                                Clock = dict:fetch(Id, Acc),
                                case Clock < Max of
                                    true ->
                                        saturn_leaf_converger:heartbeat(Id, Partition, Max, MyId),
                                        dict:store(Id, Max, Acc);
                                    false ->
                                        Acc
                                end
                            end, VVRemote0, dict:fetch_keys(VVRemote0)),
    VV1 = dict:store(MyId, Max, VV0),
    riak_core_vnode:send_command_after(?HEARTBEAT_FREQ, send_heartbeat),
    {noreply, S0#state{vv_remote=VVRemote1, vv=VV1}};

handle_command({heartbeat, Clock, Id}, _From, S0=#state{vv=VV0}) ->
    %lager:info("Hearbeadt received: ~p from ~p", [Clock, Id]),
    VV1 = dict:store(Id, Clock, VV0),
    {noreply, S0#state{vv=VV1}};

handle_command({new_lst, Partition, Clock}, _From, S0=#state{vv_lst=VV_LST0, connector=Connector0, pops=PendingOps0}) ->
    VV_LST1 = dict:store(Partition, Clock, VV_LST0),
    GST = compute_gst(VV_LST1),
    {PendingOps1, Connector1} = flush_pending_operations(PendingOps0, GST, Connector0),                    
    {noreply, S0#state{vv_lst=VV_LST1, gst=GST, pops=PendingOps1, connector=Connector1}};

handle_command(compute_times, _From, S0=#state{vv=VV, partition=Partition, pops=PendingOps0, connector=Connector0, vv_lst=VV_LST0}) ->
    LST = lists:foldl(fun(Id, Min) ->
                        min(dict:fetch(Id, VV), Min)
                     end, infinity, dict:fetch_keys(VV)),
    VV_LST1=dict:store(Partition, LST, VV_LST0),
    GST = compute_gst(VV_LST1),
    {PendingOps1, Connector1} = flush_pending_operations(PendingOps0, GST, Connector0),                    
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    GrossPrefLists = riak_core_ring:all_preflists(Ring, 1),
    lists:foreach(fun(PrefList) ->
                    case hd(PrefList) of
                        {Partition, _Node} ->
                            noop;
                        {_OtherPartition, _Node} ->
                            saturn_proxy_vnode:new_lst(hd(PrefList), Partition, LST)
                    end
                  end, GrossPrefLists),
    riak_core_vnode:send_command_after(?TIMES_FREQ, compute_times),
    {noreply, S0#state{vv_lst=VV_LST1, gst=GST, pops=PendingOps1, connector=Connector1}};

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
    S0.
    
compute_gst(VV_LST) ->
    _GST = lists:foldl(fun(Entry, Min) ->
                        min(Min, dict:fetch(Entry, VV_LST))
                       end, infinity, dict:fetch_keys(VV_LST)).

flush_pending_operations([], _GST, Connector0) ->
    {[], Connector0};

flush_pending_operations([Next|Rest]=PendingOps0, GST, Connector0) ->
    {TimeStamp, List} = Next,
    case TimeStamp =< GST of
        true ->
            Connector1 = lists:foldl(fun({Type, Payload}, Acc) ->
                                        handle_operation(Type, Payload, Acc, GST)
                                     end, Connector0, List),
            flush_pending_operations(Rest, GST, Connector1);
        false ->
            {PendingOps0, Connector0}
    end.

handle_operation(Type, Payload, Connector0, GST) ->
    case Type of
        update ->
            {BKey, Value, TimeStamp} = Payload,
            {ok, {_StoredValue, StoredTimeStamp}} = ?BACKEND_CONNECTOR:read(Connector0, {BKey}),
            case StoredTimeStamp =< TimeStamp of
                true ->
                    {ok, Connector1} = ?BACKEND_CONNECTOR:update(Connector0, {BKey, Value, TimeStamp}),
                    Connector1;
                false ->
                    Connector0
            end;
        remote_read ->
            {BKey, Sender, Client} = Payload,
            {ok, {StoredValue, StoredTimeStamp}} = ?BACKEND_CONNECTOR:read(Connector0, {BKey}),
            saturn_leaf_converger:remote_reply(Sender, BKey, StoredValue, Client, StoredTimeStamp),
            Connector0;
        remote_reply ->
            {Value, Client, Clock} = Payload,
            riak_core_vnode:reply(Client, {ok, {Value, Clock, GST}}),
            Connector0;
        _ ->
            lager:error("Unhandled pending operation of type: ~p with payload ~p", [Type, Payload]),
            Connector0
    end.
