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
         update/4,
         new_operation/5,
         clock/5,
         new_invalidation/4,
         remote_read/4,
         check_ready/1]).

-record(state, {partition,
                connector,
                preads,
                pops,
                vv,
                myid}).

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

read(Node, BKey, Clock) ->
    riak_core_vnode_master:sync_command(Node,
                                        {read, BKey, Clock},
                                        ?PROXY_MASTER).

update(Node, BKey, Value, Clock) ->
    riak_core_vnode_master:sync_command(Node,
                                        {update, BKey, Value, Clock},
                                        ?PROXY_MASTER).

clock(Node, BKey, Value, Clock, Client) ->
    riak_core_vnode_master:command(Node,
                                   {clock, BKey, Value, Clock, Client},
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).

new_operation(IndexNode, Clock, BKey, Node, Value) ->
    riak_core_vnode_master:command(IndexNode,
                                   {new_operation, Clock, BKey, Node, Value},
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).

new_invalidation(IndexNode, Clock, BKey, Node) ->
    riak_core_vnode_master:sync_command(IndexNode,
                                        {new_invalidation, Clock, BKey, Node},
                                        ?PROXY_MASTER).

remote_read(Node, BKey, Sender, RemoteClient) ->
    riak_core_vnode_master:command(Node,
                                   {remote_read, BKey, Sender, RemoteClient},
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).

init([Partition]) ->
    lager:info("Vnode init"),
    Connector = ?BACKEND_CONNECTOR:connect([Partition]),
    {ok, #state{partition=Partition,
                preads=dict:new(),
                pops=dict:new(),
                connector=Connector
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

handle_command({read, BKey, _Deps}, From, S0=#state{connector=Connector, myid=MyId, preads=PendingReads0}) ->
    case groups_manager_serv:get_closest_dcid(BKey) of
        {ok, MyId} ->
            case ?BACKEND_CONNECTOR:read(Connector, {BKey}) of
                {ok, {invalid, Ts}} ->
                    case dict:find(BKey, PendingReads0) of
                        {ok, Orddict0} ->
                            Orddict1 = orddict:append(Ts, {From, local}, Orddict0);
                        error ->   
                            Orddict1 = orddict:append(Ts, {From, local}, orddict:new())
                    end,
                    PendingReads1 = dict:store(BKey, Orddict1, PendingReads0),
                    {noreply, S0#state{preads=PendingReads1}};
                {ok, {Value, _Ts}} ->
                    {reply, {ok, Value}, S0};
                _ ->
                    lager:error("Wrong format when reading"),
                    {reply, {error, wrong_format}, S0}
            end;
        {ok, Id} ->
            %Remote read
            saturn_leaf_converger:remote_read(MyId, BKey, Id, From),
            {noreply, S0};
        {error, Reason} ->
            lager:error("BKey ~p ~p is not replicated",  [BKey, Reason]),
            {reply, {error, Reason}, S0}
    end;

handle_command({update, BKey, Value, _Deps}, From, S0=#state{myid=MyId}) ->
    saturn_leaf_converger:local_update(MyId, BKey, Value, From),
    {noreply, S0};

handle_command({clock, BKey, Value, Clock, Client}, _From, S0=#state{myid=MyId, preads=PendingReads0, connector=Connector0}) ->
    S1=case groups_manager_serv:do_replicate(BKey) of
        true ->
            {ok, Connector1} = ?BACKEND_CONNECTOR:update(Connector0, {BKey, Value, Clock}),
            PendingReads1 = flush_pending_reads(BKey, Value, PendingReads0, MyId, all),
            S0#state{connector=Connector1, preads=PendingReads1};
        false ->
            S0;
        {error, Reason1} ->
            lager:error("BKey ~p ~p in the dictionary",  [BKey, Reason1]),
            S0
    end,
    case groups_manager_serv:get_datanodes_ids(BKey) of
        {ok, Group} ->
            lists:foreach(fun(Id) ->
                            saturn_leaf_producer:new_operation(Id, Clock, BKey, MyId, Value)
                          end, Group);
        {error, Reason2} ->
            lager:error("No replication group for bkey: ~p (~p)", [BKey, Reason2])
    end,
    riak_core_vnode:reply(Client, ok),
    {noreply, S1};

handle_command({new_operation, Clock, BKey, Node, Value}, _From, S0=#state{vv=VV0, pops=PendingOps0}) ->
    lager:info("Operation: ~p", [{Clock, BKey, Node}]),
    Stable = dict:fetch(Node, VV0),
    case Clock =< Stable of
        true ->
            S1 = execute_propagated_operation(BKey, Clock, Value, S0),
            {noreply, S1};
        false ->
            PendingOps1 = dict:store({Clock, BKey, Node}, Value, PendingOps0),
            {noreply, S0#state{pops=PendingOps1}}
    end;

handle_command({new_invalidation, Clock, BKey, Node}, _From, S0=#state{connector=Connector0, vv=VV0, pops=PendingOps0}) ->
    VV1 = dict:store(Node, Clock, VV0),
    case groups_manager_serv:do_replicate(BKey) of
        true ->
            case dict:find({Clock, BKey, Node}, PendingOps0) of
                {ok, Value} ->
                    S1 = execute_propagated_operation(BKey, Clock, Value, S0),
                    PendingOps1 = dict:erase({Clock, BKey, Node}, PendingOps0),
                    {reply, ok, S1#state{vv=VV1, pops=PendingOps1}};
                error ->
                    {ok, Connector1} = ?BACKEND_CONNECTOR:update(Connector0, {BKey, invalid, Clock}),
                    {reply, ok, S0#state{vv=VV1, connector=Connector1}}
            end;
        false ->
            {reply, ok, S0#state{vv=VV1}};
        {error, Reason1} ->
            lager:error("BKey ~p ~p in the dictionary",  [BKey, Reason1]),
            {reply, ok, S0#state{vv=VV1}}
    end;

handle_command({remote_read, BKey, Sender, RemoteClient}, _From, S0=#state{connector=Connector, myid=MyId, preads=PendingReads0}) ->
    {ok, {Value, Ts}} = ?BACKEND_CONNECTOR:read(Connector, {BKey}),
    case Value of
        invalid ->
            case dict:find(BKey, PendingReads0) of
                {ok, Orddict0} ->
                    Orddict1 = orddict:append(Ts, {RemoteClient, Sender}, Orddict0);
                error ->
                    Orddict1 = orddict:append(Ts, {RemoteClient, Sender}, orddict:new())
            end,
            PendingReads1 = dict:store(BKey, Orddict1, PendingReads0),
            {noreply, S0#state{preads=PendingReads1}};
        _ ->
            saturn_leaf_converger:remote_reply(MyId, BKey, Sender, {RemoteClient, Value}),
            {noreply, S0}
    end;

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
            {ok, Entries} = groups_manager_serv:get_all_nodes_but_myself(),
            PreciseVV = lists:foldl(fun(Entry, VV) ->
                                        dict:store(Entry, 0, VV)
                                    end, dict:new(), Entries),
            S0#state{myid=MyId, vv=PreciseVV}
    end.

execute_propagated_operation(BKey, Clock, Value, S0=#state{connector=Connector0, preads=PendingReads0, myid=MyId}) ->
    {ok, {_ValueStored, Ts}} = ?BACKEND_CONNECTOR:read(Connector0, {BKey}),
    PendingReads1 = flush_pending_reads(BKey, Value, PendingReads0, MyId, Clock),
    S1 = S0#state{preads=PendingReads1},
    case Ts =< Clock of
        true ->
            {ok, Connector1} = ?BACKEND_CONNECTOR:update(Connector0, {BKey, Value, Clock}),
            S1#state{connector=Connector1};
        false ->
            S1
    end.

flush_pending_reads(BKey, Value, PendingReads0, MyId, all) ->
    case dict:find(BKey, PendingReads0) of
        {ok, Orddict0} ->
            lists:foreach(fun({_Clock, Clients}) ->
                            lists:foreach(fun({Client, Type}) ->
                                            case Type of
                                                local ->
                                                    riak_core_vnode:reply(Client, {ok, Value});
                                                Id ->
                                                     saturn_leaf_converger:remote_reply(MyId, bkey, Id, {Client, Value}) 
                                            end
                                          end, Clients)
                          end, Orddict0),
            dict:erase(BKey, PendingReads0);
        error ->
            PendingReads0
    end;

flush_pending_reads(BKey, Value, PendingReads0, MyId, Clock) ->
    case dict:find(BKey, PendingReads0) of
        {ok, Orddict0} ->
            Orddict1 = filter_clients(Orddict0, Value, Clock, MyId),
            case Orddict1 of
                [] ->
                    dict:erase(BKey, PendingReads0);
                _ ->
                    dict:store(BKey, PendingReads0)
            end;
        error ->
            PendingReads0
    end.

filter_clients([], _Value, _Clock, _MyId) ->
    [];

filter_clients([{Ts, Clients}|Rest]=Orddict, Value, Clock, MyId) ->
    case Ts=<Clock of
        true ->
            lists:foreach(fun({Client, Type}) ->
                            case Type of
                                local ->
                                    riak_core_vnode:reply(Client, {ok, Value});
                                Id ->
                                    saturn_leaf_converger:remote_reply(MyId, bkey, Id, {Client, Value}) 
                            end
                          end, Clients),
            filter_clients(Rest, Value, Clock, MyId);
        false ->
            Orddict
    end.
