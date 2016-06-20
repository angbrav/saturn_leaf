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
         set_receivers/2,
         set_tree/4,
         set_groups/2,
         set_myid/2,
         collect_stats/3,
         clean_state/1,
         check_ready/1]).

-record(state, {partition,
                staleness,
                connector,
                receivers,
                manager,
                myid}).

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

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

async_update(Node, BKey, Value, Clock, Client) ->
    riak_core_vnode_master:command(Node,
                                   {async_update, BKey, Value, Clock, Client},
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

propagate(Node, BKey, Value, TimeStamp) ->
    riak_core_vnode_master:command(Node,
                                   {propagate, BKey, Value, TimeStamp},
                                   {fsm, undefined, self()},
                                   ?PROXY_MASTER).

remote_read(Node, Label) ->
    riak_core_vnode_master:command(Node,
                                   {remote_read, Label},
                                   {fsm, undefined, self()},
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
    Name = list_to_atom(integer_to_list(Partition) ++ atom_to_list(staleness)),
    Staleness = ?STALENESS:init(Name),
    lager:info("Vnode init"),
    {ok, #state{partition=Partition,
                manager=Manager,
                staleness=Staleness,
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

handle_command({collect_stats, From, Type}, _Sender, S0=#state{staleness=Staleness}) ->
    {reply, {ok, ?STALENESS:compute_raw(Staleness, From, Type)}, S0};

handle_command(clean_state, _Sender, S0=#state{connector=Connector0, partition=Partition, staleness=Staleness}) ->
    Connector1 = ?BACKEND_CONNECTOR:clean(Connector0, Partition),
    Name = list_to_atom(integer_to_list(Partition) ++ atom_to_list(staleness)),
    Staleness1 = ?STALENESS:clean(Staleness, Name),
    {reply, ok, S0#state{staleness=Staleness1,
                         connector=Connector1}};

handle_command({read, BKey, _Clock}, From, S0) ->
    case do_read(sync, BKey, From, S0) of
        {error, Reason} ->
            {reply, {error, Reason}, S0};
        {ok, Value} ->
            {reply, {ok, Value}, S0};
        remote ->
            {noreply, S0}
    end;

handle_command({async_read, BKey, _Clock, Client}, _From, S0) ->
    case do_read(async, BKey, Client, S0) of
        {error, Reason} ->
            gen_server:reply(Client, {error, Reason}),
            {noreply, S0};
        {ok, Value} ->
            gen_server:reply(Client, {ok, Value}),
            {noreply, S0};
        remote ->
            {noreply, S0}
    end;

handle_command({update, BKey, Value, _Clock}, _From, S0) ->
    {{ok, TimeStamp}, S1} = do_update(BKey, Value, S0),
    {reply, {ok, TimeStamp}, S1};

handle_command({async_update, BKey, Value, _Clock, Client}, _From, S0) ->
    {{ok, TimeStamp}, S1} = do_update(BKey, Value, S0),
    gen_server:reply(Client, {ok, TimeStamp}),
    {noreply, S1};

handle_command({set_receivers, Receivers}, _From, S0) ->
    {reply, ok, S0#state{receivers=Receivers}};

handle_command({propagate, BKey, Value, {TimeStamp, Sender}}, _From, S0=#state{connector=Connector0, staleness=Staleness}) ->
    Staleness1 = ?STALENESS:add_update(Staleness, Sender, TimeStamp),
    {ok, Connector1} = ?BACKEND_CONNECTOR:update(Connector0, {BKey, Value, 0}),
    {noreply, S0#state{connector=Connector1, staleness=Staleness1}};
    
handle_command({remote_read, Label}, _From, S0=#state{connector=Connector, staleness=Staleness}) ->
    BKey = Label#label.bkey,
    Payload = Label#label.payload,
    Sender = Label#label.sender,
    TimeStamp = Label#label.timestamp,
    Staleness1 = ?STALENESS:add_remote(Staleness, Sender, TimeStamp),
    {ok, {Value, _Clock}} = ?BACKEND_CONNECTOR:read(Connector, {BKey}),
    Client = Payload#payload_remote.client,
    case Payload#payload_remote.type_call of
        async ->
            gen_server:reply(Client, {ok, {Value, 0}});
        sync ->
            riak_core_vnode:reply(Client, {ok, {Value, 0}});
        _ ->
            lager:error("Unknown type of call")
    end,
    {noreply, S0#state{staleness=Staleness1}};

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

do_read(Type, BKey, From, _S0=#state{connector=Connector, myid=MyId, partition=Partition, receivers=Receivers, manager=Manager}) ->
    Groups = Manager#state_manager.groups,
    Tree = Manager#state_manager.tree,
    case groups_manager:get_closest_dcid(BKey, Groups, MyId, Tree) of
        {ok, MyId} ->
            ?BACKEND_CONNECTOR:read(Connector, {BKey});
        {ok, Id} ->
            %Remote read
            %lager:info("Remote Update! Key: ~p", [BKey]),
            Label = create_label(remote_read, BKey, saturn_utilities:now_microsec(), {Partition, node()}, MyId, #payload_remote{client=From, type_call=Type}),
            Receiver = dict:fetch(Id, Receivers),
            saturn_leaf_converger:handle(Receiver, {remote_read, Label}),
            remote;
        {error, Reason} ->
            lager:error("BKey ~p ~p is not replicated",  [BKey, Reason]),
            {error, Reason}
    end.

do_update(BKey, Value, S0=#state{partition=Partition, myid=MyId, connector=Connector0, receivers=Receivers, manager=Manager}) -> 
    S1 = case groups_manager:do_replicate(BKey, Manager#state_manager.groups, MyId) of
        true ->
            {ok, Connector1} = ?BACKEND_CONNECTOR:update(Connector0, {BKey, Value, 0}),
            S0#state{connector=Connector1};
        false ->
            S0;
        {error, Reason1} ->
            lager:error("BKey ~p ~p is not in the dictionary",  [BKey, Reason1]),
            S0
    end,
    Label = create_label(update, BKey, saturn_utilities:now_microsec(), {Partition, node()}, MyId, {}),
    case groups_manager:get_datanodes_ids(BKey, Manager#state_manager.groups, MyId) of
        {ok, Group} ->
            lists:foreach(fun(Id) ->
                            Receiver = dict:fetch(Id, Receivers),
                            saturn_leaf_converger:handle(Receiver, {new_operation, Label, Value})
                          end, Group);
        {error, Reason2} ->
            lager:error("No replication group for bkey: ~p (~p)", [BKey, Reason2])
    end,
    {{ok, 0}, S1}.
