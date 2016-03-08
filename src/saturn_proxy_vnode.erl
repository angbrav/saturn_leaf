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
         propagate/4,
         heartbeat/2,
         remote_read/2,
         remote_update/5,
         check_deps/3,
         last_label/1,
         check_ready/1]).

-record(state, {partition,
                max_ts,
                connector,
                last_label,
                key_deps,
                rest_deps,
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

read(Node, BKey, Clock) ->
    riak_core_vnode_master:sync_command(Node,
                                        {read, BKey, Clock},
                                        ?PROXY_MASTER).

update(Node, BKey, Value, Clock) ->
    riak_core_vnode_master:sync_command(Node,
                                        {update, BKey, Value, Clock},
                                        ?PROXY_MASTER).

propagate(Node, BKey, Value, Metadata) ->
    riak_core_vnode_master:command(Node,
                                   {propagate, BKey, Value, Metadata},
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).

remote_read(Node, Label) ->
    riak_core_vnode_master:command(Node,
                                   {remote_read, Label},
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).

remote_update(Node, Label, Value, Deps, Client) ->
    riak_core_vnode_master:command(Node,
                                   {remote_update, Label, Value, Deps, Client},
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).

check_deps(Node, Id, Deps) ->
    riak_core_vnode_master:command(Node,
                                   {check_deps, Id, Deps},
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).

heartbeat(Node, MyId) ->
    riak_core_vnode_master:command(Node,
                                   {heartbeat, MyId},
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).

init([Partition]) ->
    lager:info("Vnode init"),
    Connector = ?BACKEND_CONNECTOR:connect([Partition]),
    {ok, #state{partition=Partition,
                last_label=none,
                key_deps=dict:new(),
                max_ts=0,
                rest_deps=dict:new(),
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

handle_command({check_deps, Id, Deps}, _From, S0=#state{connector=Connector, key_deps=Deps0, rest_deps=Rest0}) ->
    {R, Deps1} = lists:foldl(fun({BKey, Version}, {Total, Dict}=Acc) ->
                                case groups_manager_serv:do_replicate(BKey) of
                                    true ->
                                        {ok, {_Value, {VerRead, _D}}} = ?BACKEND_CONNECTOR:read(Connector, {BKey}),
                                        case Version =< VerRead of
                                            true ->
                                                Acc;
                                            false ->
                                                case dict:find(BKey, Dict) of
                                                    {ok, Orddict0} ->
                                                        Orddict1 = orddict:append(Version, Id, Orddict0);
                                                    error ->
                                                        Orddict1 = orddict:append(Version, Id, orddict:new())
                                                end,
                                                {Total + 1, dict:store(BKey, Orddict1, Dict)}
                                                end;
                                    false ->
                                        Acc
                                end
                              end, {0, Deps0}, Deps),
    case R of
        0 ->
            DocIdx = riak_core_util:chash_key({?BUCKET_COPS, Id}),
            PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?COPS_SERVICE),
            [{IndexNode, _Type}] = PrefList,
            saturn_cops_vnode:deps_checked(IndexNode, Id),
            {noreply, S0};
        _ ->
            Rest1 = dict:store(Id, R, Rest0),
            {noreply, S0#state{key_deps=Deps1, rest_deps=Rest1}}
    end; 

handle_command({read, BKey, Deps}, From, S0=#state{connector=Connector, myid=MyId, partition=Partition}) ->
    case groups_manager_serv:get_closest_dcid(BKey) of
        {ok, MyId} ->
            {ok, Value} = ?BACKEND_CONNECTOR:read(Connector, {BKey}),
            {reply, {ok, Value}, S0};
        {ok, Id} ->
            %Remote read
            Label = create_label(remote_read, BKey, ts, {Partition, node()}, MyId, #payload_remote{client=From, deps=Deps}),
            saturn_leaf_converger:handle(Id, {remote_read, Label}),
            {noreply, S0};
        {error, Reason} ->
            lager:error("BKey ~p ~p is not replicated",  [BKey, Reason]),
            {reply, {error, Reason}, S0}
    end;

handle_command({update, BKey, Value, Deps}, From, S0=#state{partition=Partition, myid=MyId}) ->
    case groups_manager_serv:get_closest_dcid(BKey) of
        {ok, MyId} ->
            {Version, S1} = do_put(BKey, Value, Deps, S0),
            {reply, {ok, Version}, S1};
        {ok, Id} ->
            Label = create_label(remote_update, BKey, none, {Partition, node()}, MyId, #payload_remote_update{deps=Deps, client=From}),
            saturn_leaf_converger:handle(Id, {remote_update, Label, Value}),
            {noreply, S0};
        {error, Reason1} ->
            lager:error("BKey ~p ~p is not replicated",  [BKey, Reason1]),
            {reply, {error, Reason1}, S0}
    end;

handle_command({remote_update, BKey, Value, Deps, Client}, _From, S0) ->
    {Version, S1} = do_put(BKey, Value, Deps, S0),
    riak_core_vnode:reply(Client, {ok, Version}),
    {noreply, S1};

handle_command({propagate, BKey, Value, {TimeStamp, Deps}}, _From, S0=#state{connector=Connector0, key_deps=KeyDeps0}) ->
    {ok, {_OldValue, {OldVersion, _OldDeps}}} = ?BACKEND_CONNECTOR:read(Connector0, {BKey}),
    case OldVersion =< TimeStamp of
        true ->
            {ok, Connector1} = ?BACKEND_CONNECTOR:update(Connector0, {BKey, Value, {TimeStamp, Deps}}),
            S1 = case dict:find(BKey, KeyDeps0) of
                    {ok, Orddict0} ->
                        handle_pending_deps(Orddict0, BKey, TimeStamp, S0#state{connector=Connector1});
                    error ->
                        S0#state{connector=Connector1}
                end,
            {noreply, S1};
        false ->
            {noreply, S0}
    end;
    
handle_command({remote_read, Label}, _From, S0=#state{connector=Connector, myid=MyId, partition=Partition}) ->
    BKey = Label#label.bkey,
    Payload = Label#label.payload,
    Sender = Label#label.sender,
    {ok, {Value,{Version, Deps}}} = ?BACKEND_CONNECTOR:read(Connector, {BKey}),
    Client = Payload#payload_remote.client,
    NewLabel = create_label(remote_reply, BKey, Version, {Partition, node()}, MyId, #payload_reply{value=Value, deps=Deps, client=Client}),
    saturn_leaf_converger:handle(Sender, {remote_reply, NewLabel}),
    {noreply, S0};

handle_command({heartbeat, MyId}, _From, S0=#state{partition=Partition, max_ts=MaxTS0}) ->
    Clock = max(saturn_utilities:now_microsec(), MaxTS0+1),
    saturn_leaf_producer:partition_heartbeat(MyId, Partition, Clock),
    %riak_core_vnode:send_command_after(?HEARTBEAT_FREQ, {heartbeat, MyId}),
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
    
create_label(Operation, BKey, TimeStamp, Node, Id, Payload) ->
    #label{operation=Operation,
           bkey=BKey,
           timestamp=TimeStamp,
           node=Node,
           sender=Id,
           payload=Payload
           }.

handle_pending_deps([], BKey, _Version, S0=#state{key_deps=KeyDeps0}) ->
    S0#state{key_deps=dict:erase(BKey, KeyDeps0)};

handle_pending_deps([{PVersion, Ids}|Rest]=Orddict, BKey, Version, S0=#state{rest_deps=Rest0, key_deps=KeyDeps0}) ->
    case PVersion =< Version of
        true ->
            Rest1 = lists:foldl(fun(Id, Acc) ->
                                    case dict:fetch(Id, Acc) of
                                        1 ->
                                            DocIdx = riak_core_util:chash_key({?BUCKET_COPS, Id}),
                                            PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?COPS_SERVICE),
                                            [{IndexNode, _Type}] = PrefList,
                                            saturn_cops_vnode:deps_checked(IndexNode, Id),
                                            dict:erase(Id, Acc);
                                        Left ->
                                            dict:store(Id, Left-1, Acc)
                                    end
                                end, Rest0, Ids),
            handle_pending_deps(Rest, BKey, Version, S0#state{rest_deps=Rest1});
        false ->
            S0#state{key_deps=dict:store(BKey, Orddict, KeyDeps0)}
    end.

do_put(BKey, Value, Deps, S0=#state{partition=Partition, myid=MyId, connector=Connector0, key_deps=KeyDeps0}) ->
    {ok, {_OldValue, {OldVersion, _OldDeps}}} = ?BACKEND_CONNECTOR:read(Connector0, {BKey}),
    Version = OldVersion + 1,
    {ok, Connector1} = ?BACKEND_CONNECTOR:update(Connector0, {BKey, Value, {Version, Deps}}),
    S1 = case dict:find(BKey, KeyDeps0) of
            {ok, Orddict0} ->
                handle_pending_deps(Orddict0, BKey, Version, S0#state{connector=Connector1});
            error ->
                S0#state{connector=Connector1}
         end,
    Label = create_label(update, BKey, Version, {Partition, node()}, MyId, Deps),
    case groups_manager_serv:get_datanodes_ids(BKey) of
        {ok, Group} ->
            lists:foreach(fun(Id) ->
                saturn_leaf_converger:handle(Id, {new_operation, Label, Value})
                          end, Group);
        {error, Reason2} ->
            lager:error("No replication group for bkey: ~p (~p)", [BKey, Reason2])
    end,
    {Version, S1}.
