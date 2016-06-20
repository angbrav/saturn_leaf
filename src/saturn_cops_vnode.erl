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
-module(saturn_cops_vnode).
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

-export([update/4,
         remote_read/3,
         remote_update/4,
         remote_reply/3,
         deps_checked/2,
         clean_state/1,
         check_ready/1]).

-record(state, {ops,
                values,
                partition}).

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

update(Node, Id, Label, Value) ->
    riak_core_vnode_master:command(Node,
                                   {update, Id, Label, Value},
                                   {fsm, undefined, self()},
                                   ?COPS_MASTER).

remote_read(Node, Id, Label) ->
    riak_core_vnode_master:command(Node,
                                   {remote_read, Id, Label},
                                   {fsm, undefined, self()},
                                   ?COPS_MASTER).

remote_update(Node, Id, Label, Value) ->
    riak_core_vnode_master:command(Node,
                                   {remote_update, Id, Label, Value},
                                   {fsm, undefined, self()},
                                   ?COPS_MASTER).

remote_reply(Node, Id, Label) ->
    riak_core_vnode_master:command(Node,
                                   {remote_reply, Id, Label},
                                   {fsm, undefined, self()},
                                   ?COPS_MASTER).

deps_checked(Node, Id) ->
    riak_core_vnode_master:command(Node,
                                   {deps_checked, Id},
                                   {fsm, undefined, self()},
                                   ?COPS_MASTER).

clean_state(Node) ->
    riak_core_vnode_master:sync_command(Node,
                                        clean_state,
                                        ?PROXY_MASTER).

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
        ?COPS_MASTER,
        infinity),
    case Result of
        true ->
            check_ready_partition(Rest, Function);
        false ->
            false
    end.

init([Partition]) ->
    Name1 = list_to_atom(integer_to_list(Partition) ++ atom_to_list(ops_cops)),
    Ops = ets:new(Name1, [set, named_table]),
    Name2 = list_to_atom(integer_to_list(Partition) ++ atom_to_list(values_cops)),
    Values = ets:new(Name2, [set, named_table]),
    {ok, #state{partition=Partition,
                ops=Ops,
                values=Values 
               }}.

handle_command(clean_state, _Sender, S0=#state{partition=Partition, ops=Ops0, values=Values0}) ->
    true = ets:delete(Ops0),
    true = ets:delete(Values0),
    Name1 = list_to_atom(integer_to_list(Partition) ++ atom_to_list(ops_cops)),
    Ops = ets:new(Name1, [set, named_table]),
    Name2 = list_to_atom(integer_to_list(Partition) ++ atom_to_list(values_cops)),
    Values = ets:new(Name2, [set, named_table]),
    {reply, ok, S0#state{ops=Ops,
                       values=Values}};

handle_command({check_tables_ready}, _Sender, SD0) ->
    {reply, true, SD0};

handle_command({remote_read, Id, Label}, _From, S0=#state{ops=Ops, values=Values}) ->
    Payload = Label#label.payload,
    Deps = Payload#payload_remote.deps,
    case Deps of
        [] ->
            ok = handle_pending_op(Id, Label, Values);
        _ ->
            Split = scatter_deps(Deps),
            ok = send_checking_messages(Split, Id, Label, Ops)
    end,
    {noreply, S0};

handle_command({remote_reply, Id, Label}, _From, S0=#state{ops=Ops, values=Values}) ->
    Payload = Label#label.payload,
    Deps = Payload#payload_reply.deps,
    case Deps of
        [] ->
            ok = handle_pending_op(Id, Label, Values);
        _ ->
            Split = scatter_deps(Deps),
            ok = send_checking_messages(Split, Id, Label, Ops)
    end,
    {noreply, S0};

handle_command({update, Id, Label, Value}, _From, S0=#state{ops=Ops, values=Values}) ->
    {Deps, _Time} = Label#label.payload,
    true = ets:insert(Values, {Id, Value}),
    case Deps of
        [] ->
            ok = handle_pending_op(Id, Label, Values);
        _ ->
            Split = scatter_deps(Deps),
            ok = send_checking_messages(Split, Id, Label, Ops)
    end,
    {noreply, S0};

handle_command({remote_update, Id, Label, Value}, _From, S0=#state{ops=Ops, values=Values}) ->
    Payload = Label#label.payload,
    Deps = Payload#payload_remote_update.deps,
    true = ets:insert(Values, {Id, Value}),
    case Deps of
        [] ->
            ok = handle_pending_op(Id, Label, Values);
        _ ->
            Split = scatter_deps(Deps),
            ok = send_checking_messages(Split, Id, Label, Ops)
    end,
    {noreply, S0};

handle_command({deps_checked, Id}, _From, S0=#state{ops=Ops, values=Values}) ->
    case ets:lookup(Ops, Id) of
        [] ->
            lager:error("deps_checked received for id: ~p, but no pending with such an id", [Id]),
            {noreply, S0};
        [{Id, {Rest, Label}}] ->
            case Rest of
                1 ->
                    true = ets:delete(Ops, Id),
                    ok = handle_pending_op(Id, Label, Values);
                _ ->
                    true = ets:insert(Ops, {Id, {Rest - 1, Label}})
            end,
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

handle_pending_op(Id, Label, Values) ->
    %lager:info("Label: ~p", [Label]),
    BKey = Label#label.bkey,
    DocIdx = riak_core_util:chash_key(BKey),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
    [{IndexNode, _Type}] = PrefList,
    case Label#label.operation of
        update ->
            case ets:lookup(Values, Id) of
                [] ->
                    lager:error("deps_checked received for id: ~p, but no pending value with such an id", [Id]),
                    {error, no_value};
                [{Id, Value}] ->
                    saturn_proxy_vnode:propagate(IndexNode, BKey, Value, {Label#label.timestamp, Label#label.payload, Label#label.sender}),
                    true = ets:delete(Values, Id),
                    ok
            end;
        remote_read ->
                saturn_proxy_vnode:remote_read(IndexNode, Label),
                ok;
        remote_reply ->
                Payload = Label#label.payload,
                Client = Payload#payload_reply.client,
                Value = Payload#payload_reply.value,
                Deps = Payload#payload_reply.deps,
                TypeCall = Payload#payload_reply.type_call,
                case TypeCall of
                    sync ->
                        riak_core_vnode:reply(Client, {ok, {Value, Deps}});
                    async ->
                        gen_server:reply(Client, {ok, {Value, Deps}})
                end,
                ok;
        remote_update ->
                case ets:lookup(Values, Id) of
                [] ->
                    lager:error("deps_checked received for id: ~p, but no pending value with such an id", [Id]),
                    {error, no_value};
                [{Id, Value}] ->
                    Payload = Label#label.payload,
                    Client = Payload#payload_remote_update.client,
                    Deps = Payload#payload_remote_update.deps,
                    TypeCall = Payload#payload_remote_update.type_call,
                    saturn_proxy_vnode:remote_update(IndexNode, BKey, Value, Deps, Client, TypeCall),
                    true = ets:delete(Values, Id),
                    ok
            end;
        _ ->
                lager:error("Unknown operation: ~p", [Label#label.operation]),
                {error, unknown_operation}
    end.

scatter_deps(Deps) ->
    lists:foldl(fun({BKey, _Version}=Dependency, Acc) ->
                    DocIdx = riak_core_util:chash_key(BKey),
                    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
                    [{IndexNode, _Type}] = PrefList,
                    dict:append(IndexNode, Dependency, Acc)
                end, dict:new(), Deps).

send_checking_messages(Split, Id, Label, Ops) ->
    Nodes = dict:fetch_keys(Split),
    lists:foreach(fun(IndexNode) ->
                    List = dict:fetch(IndexNode, Split),
                    saturn_proxy_vnode:check_deps(IndexNode, Id, List)
                  end, Nodes),
    true = ets:insert(Ops, {Id, {length(Nodes), Label}}),
    ok.
