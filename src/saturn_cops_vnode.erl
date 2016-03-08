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
         check_ready/1]).

-record(state, {ops :: dict(),
                values :: dict(),
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
    {ok, #state{partition=Partition,
                ops=dict:new(),
                values=dict:new() 
               }}.

handle_command({check_tables_ready}, _Sender, SD0) ->
    {reply, true, SD0};

handle_command({remote_read, Id, Label}, _From, S0) ->
    Payload = Label#label.payload,
    Deps = Payload#payload_remote.deps,
    case Deps of
        [] ->
            S1 = handle_pending_op(Id, Label, S0);
        _ ->
            Split = scatter_deps(Deps),
            S1 = send_checking_messages(Split, Id, Label, S0)
    end,
    {noreply, S1};

handle_command({remote_reply, Id, Label}, _From, S0) ->
    Payload = Label#label.payload,
    Deps = Payload#payload_reply.deps,
    case Deps of
        [] ->
            S1 = handle_pending_op(Id, Label, S0);
        _ ->
            Split = scatter_deps(Deps),
            S1 = send_checking_messages(Split, Id, Label, S0)
    end,
    {noreply, S1};

handle_command({update, Id, Label, Value}, _From, S0=#state{values=Values0}) ->
    Deps = Label#label.payload,
    Values1 = dict:store(Id, Value, Values0),
    case Deps of
        [] ->
            S1 = handle_pending_op(Id, Label, S0#state{values=Values1});
        _ ->
            Split = scatter_deps(Deps),
            S1 = send_checking_messages(Split, Id, Label, S0#state{values=Values1})
    end,
    {noreply, S1};

handle_command({remote_update, Id, Label, Value}, _From, S0=#state{values=Values0}) ->
    Payload = Label#label.payload,
    Deps = Payload#payload_remote_update.deps,
    Values1 = dict:store(Id, Value, Values0),
    case Deps of
        [] ->
            S1 = handle_pending_op(Id, Label, S0#state{values=Values1});
        _ ->
            Split = scatter_deps(Deps),
            S1 = send_checking_messages(Split, Id, Label, S0#state{values=Values1})
    end,
    {noreply, S1};

handle_command({deps_checked, Id}, _From, S0=#state{ops=Ops0}) ->
    case dict:find(Id, Ops0) of
        {ok, {Rest, Label}} ->
            case Rest of
                1 ->
                    Ops1 = dict:erase(Id, Ops0),
                    S1=handle_pending_op(Id, Label, S0#state{ops=Ops1});
                _ ->
                    Ops1 = dict:store(Id, {Rest - 1, Label}, Ops0),
                    S1=S0#state{ops=Ops1}
            end,
            {noreply, S1};
        error ->
            lager:error("deps_checked received for id: ~p, but no pending with such an id", [Id]),
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

handle_pending_op(Id, Label, S0=#state{values=Values0}) ->
    %lager:info("Label: ~p", [Label]),
    BKey = Label#label.bkey,
    DocIdx = riak_core_util:chash_key(BKey),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
    [{IndexNode, _Type}] = PrefList,
    case Label#label.operation of
        update ->
            case dict:find(Id, Values0) of
                {ok, Value} ->
                    saturn_proxy_vnode:propagate(IndexNode, BKey, Value, {Label#label.timestamp, Label#label.payload}),
                    Values1 = dict:erase(Id, Values0),
                    S0#state{values=Values1};
                error ->
                    lager:error("deps_checked received for id: ~p, but no pending value with such an id", [Id]),
                    S0
            end;
        remote_read ->
                saturn_proxy_vnode:remote_read(IndexNode, Label),
                S0;
        remote_reply ->
                Payload = Label#label.payload,
                Client = Payload#payload_reply.client,
                Value = Payload#payload_reply.value,
                Deps = Payload#payload_reply.deps,
                riak_core_vnode:reply(Client, {ok, {Value, Deps}}),
                S0;
        remote_update ->
                case dict:find(Id, Values0) of
                {ok, Value} ->
                    Payload = Label#label.payload,
                    Client = Payload#payload_remote_update.client,
                    Deps = Payload#payload_remote_update.deps,
                    saturn_proxy_vnode:remote_update(IndexNode, BKey, Value, Deps, Client),
                    Values1 = dict:erase(Id, Values0),
                    S0#state{values=Values1};
                error ->
                    lager:error("deps_checked received for id: ~p, but no pending value with such an id", [Id]),
                    S0
            end;
        _ ->
                lager:error("Unknown operation: ~p", [Label#label.operation]),
                S0
    end.

scatter_deps(Deps) ->
    lists:foldl(fun({BKey, _Version}=Dependency, Acc) ->
                    DocIdx = riak_core_util:chash_key(BKey),
                    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
                    [{IndexNode, _Type}] = PrefList,
                    dict:append(IndexNode, Dependency, Acc)
                end, dict:new(), Deps).

send_checking_messages(Split, Id, Label, S0=#state{ops=Ops0}) ->
    Nodes = dict:fetch_keys(Split),
    lists:foreach(fun(IndexNode) ->
                    List = dict:fetch(IndexNode, Split),
                    saturn_proxy_vnode:check_deps(IndexNode, Id, List)
                  end, Nodes),
    Ops1 = dict:store(Id, {length(Nodes), Label}, Ops0),
    S0#state{ops=Ops1}.
    
