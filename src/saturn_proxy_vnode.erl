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
         async_read/4,
         async_update/5,
         propagate/4,
         remote_read/2,
         remote_update/6,
         check_deps/3,
         last_label/1,
         set_receivers/2,
         set_tree/4,
         set_groups/2,
         set_myid/2,
         clean_state/1,
         check_ready/1]).

-record(state, {partition,
                max_ts,
                connector,
                last_label,
                key_deps,
                receivers,
                rest_deps,
                manager,
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

async_read(Node, BKey, Clock, Client) ->
    riak_core_vnode_master:command(Node,
                                   {async_read, BKey, Clock, Client},
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).

async_update(Node, BKey, Value, Clock, Client) ->
    riak_core_vnode_master:command(Node,
                                   {async_update, BKey, Value, Clock, Client},
                                   {fsm, undefined, self()},
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

remote_update(Node, Label, Value, Deps, Client, TypeCall) ->
    riak_core_vnode_master:command(Node,
                                   {remote_update, Label, Value, Deps, Client, TypeCall},
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).

check_deps(Node, Id, Deps) ->
    riak_core_vnode_master:command(Node,
                                   {check_deps, Id, Deps},
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).

set_receivers(Node, Receivers) ->
    riak_core_vnode_master:sync_command(Node,
                                        {set_receivers, Receivers},
                                        ?PROXY_MASTER).

set_myid(Node, MyId) ->
    riak_core_vnode_master:sync_command(Node,
                                        {set_myid, MyId},
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

init([Partition]) ->
    Manager = groups_manager:init_state(integer_to_list(Partition)),
    Name1 = list_to_atom(integer_to_list(Partition) ++ atom_to_list(key_deps_cops)),
    KeyDeps = ets:new(Name1, [set, named_table, private]),
    Name2 = list_to_atom(integer_to_list(Partition) ++ atom_to_list(rest_deps_cops)),
    RestDeps = ets:new(Name2, [set, named_table, private]),
    Connector = ?BACKEND_CONNECTOR:connect([Partition]),
    lager:info("Vnode init"),
    {ok, #state{partition=Partition,
                last_label=none,
                key_deps=KeyDeps,
                max_ts=0,
                manager=Manager,
                rest_deps=RestDeps,
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


handle_command(clean_state, _Sender, S0=#state{partition=Partition, key_deps=KeyDeps0, rest_deps=RestDeps0, connector=Connector0}) ->
    true = ets:delete(KeyDeps0),
    true = ets:delete(RestDeps0),
    Name1 = list_to_atom(integer_to_list(Partition) ++ atom_to_list(key_deps_cops)),
    KeyDeps = ets:new(Name1, [set, named_table, private]),
    Name2 = list_to_atom(integer_to_list(Partition) ++ atom_to_list(rest_deps_cops)),
    RestDeps = ets:new(Name2, [set, named_table, private]),
    Connector = ?BACKEND_CONNECTOR:clean(Connector0, Partition),
    {reply, ok, S0#state{last_label=none,
                       key_deps=KeyDeps,
                       max_ts=0,
                       rest_deps=RestDeps,
                       connector=Connector}};

handle_command({set_myid, MyId}, _Sender, S0) ->
    {reply, true, S0#state{myid=MyId}};

handle_command({check_tables_ready}, _Sender, SD0) ->
    {reply, true, SD0};

handle_command({set_tree, Paths, Tree, NLeaves}, _From, S0=#state{manager=Manager}) ->
    {reply, ok, S0#state{manager=Manager#state_manager{tree=Tree, paths=Paths, nleaves=NLeaves}}};

handle_command({set_groups, Groups}, _From, S0=#state{manager=Manager}) ->
    Table = Manager#state_manager.groups,
    ok = groups_manager:set_groups(Table, Groups),
    {reply, ok, S0};

handle_command({check_deps, Id, Deps}, _From, S0=#state{connector=Connector, key_deps=KeyDeps, rest_deps=Rest, manager=Manager, myid=MyId}) ->
    R = lists:foldl(fun({BKey, Version}, Total) ->
                        case groups_manager:do_replicate(BKey, Manager#state_manager.groups, MyId) of
                            true ->
                                {ok, {_Value, {VerRead, _D}}} = ?BACKEND_CONNECTOR:read(Connector, {BKey}),
                                case Version =< VerRead of
                                    true ->
                                        Total;
                                    false ->
                                        case ets:lookup(KeyDeps, BKey) of
                                            [] ->
                                                Orddict1 = orddict:append(Version, Id, orddict:new());
                                            [{BKey, Orddict0}] ->
                                                Orddict1 = orddict:append(Version, Id, Orddict0)
                                        end,
                                        true = ets:insert(KeyDeps, {BKey, Orddict1}),
                                        Total + 1
                                end;
                            false ->
                                Total
                        end
                    end, 0, Deps),
    case R of
        0 ->
            DocIdx = riak_core_util:chash_key({?BUCKET_COPS, Id}),
            PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?COPS_SERVICE),
            [{IndexNode, _Type}] = PrefList,
            saturn_cops_vnode:deps_checked(IndexNode, Id),
            {noreply, S0};
        _ ->
            true = ets:insert(Rest, {Id, R}),
            {noreply, S0}
    end; 

handle_command({read, BKey, Deps}, From, S0) ->
    case do_read(sync, BKey, Deps, From, S0) of
        {ok, Value} ->
            {reply, {ok, Value}, S0};
        remote ->
            {noreply, S0};
        {error, Reason} ->
            {reply, {error, Reason}, S0}
    end;

handle_command({async_read, BKey, Deps, Client}, _From, S0) ->
    case do_read(async, BKey, Deps, Client, S0) of
        {ok, Value} ->  
            gen_server:reply(Client, {ok, Value}),
            {noreply, S0};
        remote ->
            {noreply, S0};
        {error, Reason} ->
            gen_server:reply(Client, {error, Reason}),
            {noreply, S0}
    end;

handle_command({update, BKey, Value, Deps}, From, S0) ->
    case do_update(sync, BKey, Value, Deps, From, S0) of
        {ok, Version, S1} ->
            {reply, {ok, Version}, S1};
        remote ->
            {noreply, S0};
        {error, Reason} ->
            {reply, {error, Reason}, S0}
    end;

handle_command({async_update, BKey, Value, Deps, Client}, _From, S0) ->
    case do_update(async, BKey, Value, Deps, Client, S0) of
        {ok, Version, S1} ->
            gen_server:reply(Client, {ok, Version}),
            {noreply, S1};
        remote ->
            {noreply, S0};
        {error, Reason} ->
            gen_server:reply(Client, {error, Reason}),
            {noreply, S0}
    end;

handle_command({set_receivers, Receivers}, _From, S0) ->
    {reply, ok, S0#state{receivers=Receivers}};

handle_command({remote_update, BKey, Value, Deps, Client, TypeCall}, _From, S0=#state{partition=Partition, myid=MyId, receivers=Receivers, key_deps=KeyDeps, rest_deps=RestDeps, connector=Connector0, manager=Manager}) ->
    {Version, Connector1} = do_put(BKey, Value, Deps, Partition, MyId, Connector0, KeyDeps, RestDeps, Receivers, Manager),
    case TypeCall of
        sync ->
            riak_core_vnode:reply(Client, {ok, Version});
        async ->
            gen_server:reply(Client, {ok, Version})
    end,
    {noreply, S0#state{connector=Connector1}};

handle_command({propagate, BKey, Value, {TimeStamp, Deps}}, _From, S0=#state{connector=Connector0, key_deps=KeyDeps, rest_deps=RestDeps}) ->
    %lager:info("Received remote update: ~p", [{BKey, Value}]),
    {ok, {_OldValue, {OldVersion, _OldDeps}}} = ?BACKEND_CONNECTOR:read(Connector0, {BKey}),
    case OldVersion =< TimeStamp of
        true ->
            {ok, Connector1} = ?BACKEND_CONNECTOR:update(Connector0, {BKey, Value, {TimeStamp, Deps}}),
            case ets:lookup(KeyDeps, BKey) of
                [] ->
                    noop;
                [{BKey, Orddict0}] ->
                    handle_pending_deps(Orddict0, BKey, TimeStamp, KeyDeps, RestDeps)
            end,
            {noreply, S0#state{connector=Connector1}};
        false ->
            {noreply, S0}
    end;
    
handle_command({remote_read, Label}, _From, S0=#state{connector=Connector, myid=MyId, partition=Partition, receivers=Receivers}) ->
    BKey = Label#label.bkey,
    Payload = Label#label.payload,
    Sender = Label#label.sender,
    {ok, {Value,{Version, Deps}}} = ?BACKEND_CONNECTOR:read(Connector, {BKey}),
    Client = Payload#payload_remote.client,
    TypeCall = Payload#payload_remote.type_call,
    NewLabel = create_label(remote_reply, BKey, Version, {Partition, node()}, MyId, #payload_reply{value=Value, deps=Deps, client=Client, type_call=TypeCall}),
    Receiver = dict:fetch(Sender, Receivers),
    saturn_leaf_converger:handle(Receiver, {remote_reply, NewLabel}),
    {noreply, S0};

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

handle_pending_deps([], BKey, _Version, KeyDeps, _RestDeps) ->
    true = ets:delete(KeyDeps, BKey);

handle_pending_deps([{PVersion, Ids}|Rest]=Orddict, BKey, Version, KeyDeps, RestDeps) ->
    case PVersion =< Version of
        true ->
            lists:foreach(fun(Id) ->
                            case ets:lookup(RestDeps, Id) of
                                [{Id, 1}] ->
                                    DocIdx = riak_core_util:chash_key({?BUCKET_COPS, Id}),
                                    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?COPS_SERVICE),
                                    [{IndexNode, _Type}] = PrefList,
                                    saturn_cops_vnode:deps_checked(IndexNode, Id),
                                    true = ets:delete(RestDeps, Id);
                                [{Id, Left}] ->
                                    true = ets:insert(RestDeps, {Id, Left-1})
                            end
                          end, Ids),
            handle_pending_deps(Rest, BKey, Version, KeyDeps, RestDeps);
        false ->
            true = ets:insert(KeyDeps, {BKey, Orddict})
    end.

do_read(Type, BKey, Deps, From, _S0=#state{connector=Connector, myid=MyId, partition=Partition, receivers=Receivers, manager=Manager}) ->
    Groups = Manager#state_manager.groups,
    Tree = Manager#state_manager.tree,
    case groups_manager:get_closest_dcid(BKey, Groups, MyId, Tree) of
        {ok, MyId} ->
            {ok, Value} = ?BACKEND_CONNECTOR:read(Connector, {BKey}),
            {ok, Value};
        {ok, Id} ->
            %Remote read
            Label = create_label(remote_read, BKey, ts, {Partition, node()}, MyId, #payload_remote{client=From, deps=Deps, type_call=Type}),
            Receiver = dict:fetch(Id, Receivers),
            saturn_leaf_converger:handle(Receiver, {remote_read, Label}),
            remote;
        {error, Reason} ->
            lager:error("BKey ~p ~p is not replicated",  [BKey, Reason]),
            {error, Reason}
    end.

do_update(Type, BKey, Value, Deps, From, S0=#state{partition=Partition, myid=MyId, receivers=Receivers, key_deps=KeyDeps, rest_deps=RestDeps, connector=Connector0, manager=Manager}) ->
    Groups = Manager#state_manager.groups,
    Tree = Manager#state_manager.tree,
    case groups_manager:get_closest_dcid(BKey, Groups, MyId, Tree) of
        {ok, MyId} ->
            {Version, Connector1} = do_put(BKey, Value, Deps, Partition, MyId, Connector0, KeyDeps, RestDeps, Receivers, Manager),
            {ok, Version, S0#state{connector=Connector1}};
        {ok, Id} ->
            Label = create_label(remote_update, BKey, none, {Partition, node()}, MyId, #payload_remote_update{deps=Deps, client=From, type_call=Type}),
            Receiver = dict:fetch(Id, Receivers),
            saturn_leaf_converger:handle(Receiver, {remote_update, Label, Value}),
            remote;
        {error, Reason1} ->
            lager:error("BKey ~p ~p is not replicated",  [BKey, Reason1]),
            {error, Reason1}
    end.

do_put(BKey, Value, Deps, Partition, MyId, Connector0, KeyDeps, RestDeps, Receivers, Manager) ->
    {ok, {_OldValue, {OldVersion, _OldDeps}}} = ?BACKEND_CONNECTOR:read(Connector0, {BKey}),
    Version = OldVersion + 1,
    {ok, Connector1} = ?BACKEND_CONNECTOR:update(Connector0, {BKey, Value, {Version, Deps}}),
    case ets:lookup(KeyDeps, BKey) of
        [] ->
            noop;
        [{BKey, Orddict0}] ->
            handle_pending_deps(Orddict0, BKey, Version, KeyDeps, RestDeps)
    end,
    Label = create_label(update, BKey, Version, {Partition, node()}, MyId, Deps),
    case groups_manager:get_datanodes_ids(BKey, Manager#state_manager.groups, MyId) of
        {ok, Group} ->
            lists:foreach(fun(Id) ->
                Receiver = dict:fetch(Id, Receivers),
                saturn_leaf_converger:handle(Receiver, {new_operation, Label, Value})
                          end, Group);
        {error, Reason2} ->
            lager:error("No replication group for bkey: ~p (~p)", [BKey, Reason2])
    end,
    {Version, Connector1}.
