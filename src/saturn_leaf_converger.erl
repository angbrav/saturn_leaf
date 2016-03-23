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
-module(saturn_leaf_converger).
-behaviour(gen_server).

-include("saturn_leaf.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).
-export([handle/2]).

-record(state, {labels_queue :: queue(),
                ops,
                queue_len,
                myid}).
               
reg_name(MyId) ->  list_to_atom(integer_to_list(MyId) ++ atom_to_list(?MODULE)). 

start_link(MyId) ->
    gen_server:start({global, reg_name(MyId)}, ?MODULE, [MyId], []).

handle(MyId, Message) ->
    %lager:info("Message received: ~p", [Message]),
    gen_server:call({global, reg_name(MyId)}, Message, infinity).

init([MyId]) ->
    Ops = ets:new(operations_converger, [set, named_table]),
    {ok, #state{labels_queue=queue:new(),
                ops=Ops,
                queue_len=0,
                myid=MyId}}.

handle_call({new_stream, Stream, _SenderId}, _From, S0=#state{labels_queue=Labels0, queue_len=QL0, ops=Ops, myid=MyId}) ->
    %lager:info("New stream received. Label: ~p", Stream),
    case QL0 of
        0 ->
            {Labels1, QL1} = flush_list(Stream, Ops, MyId),
            {reply, ok, S0#state{labels_queue=Labels1, queue_len=QL1}}; 
        _ ->
            {Labels1, Total} = lists:foldl(fun(Elem, Sum) ->
                                            {queue:in(Elem, Labels0), Sum + 1}
                                           end, 0, Stream),
            {reply, ok, S0#state{labels_queue=Labels1, queue_len=QL0+Total}}
    end;

handle_call({new_operation, Label, Value}, _From, S0=#state{labels_queue=Labels0, ops=Ops, queue_len=QL0, myid=MyId}) ->
    %lager:info("New operation received. Label: ~p", [Label]),
    case queue:peek(Labels0) of
        {value, Label} ->
            ok = execute_operation(Label, Value),
            Labels1 = queue:drop(Labels0),
            {Labels2, QL1} = flush_queue(Labels1, QL0-1, Ops, MyId),
            {reply, ok, S0#state{labels_queue=Labels2, queue_len=QL1}};
        _ ->
            true = ets:insert(Ops, {Label, Value}),
            {reply, ok, S0}
    end.

handle_cast(_Info, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

execute_operation(Label, Value) ->
    BKey = Label#label.bkey,
    Clock = Label#label.timestamp,
    DocIdx = riak_core_util:chash_key(BKey),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
    [{IndexNode, _Type}] = PrefList,
    saturn_proxy_vnode:propagate(IndexNode, BKey, Value, Clock).

flush_list([], _Ops, _MyId) ->
    {queue:new(), 0};

flush_list([Label|Rest]=List, Ops, MyId) ->
    case handle_label(Label, Ops, MyId) of
        true ->
            flush_list(Rest, Ops, MyId);
        false ->
            {queue:from_list(List), length(List)}
    end.

flush_queue(Queue, Length, Ops, MyId) ->
    case queue:peek(Queue) of
        {value, Label} ->
            case handle_label(Label, Ops, MyId) of
                true ->
                    flush_queue(queue:drop(Queue), Length - 1, Ops, MyId);
                false ->
                    {Queue, Length}
            end;
        _ ->
            {queue:new(), 0}
    end.

handle_label(Label, Ops, MyId) ->
    case Label#label.operation of
        remote_read ->
            Payload = Label#label.payload,
            Destination = Payload#payload_remote.to,
            case is_sent_to_me(Destination, MyId) of
                true ->
                    BKey = Label#label.bkey,
                    DocIdx = riak_core_util:chash_key(BKey),
                    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
                    [{IndexNode, _Type}] = PrefList,
                    saturn_proxy_vnode:remote_read(IndexNode, Label);
                false ->
                    noop
            end,
            true;
        remote_reply ->
            Payload = Label#label.payload,
            Destination = Payload#payload_reply.to,
            case is_sent_to_me(Destination, MyId) of
                true ->
                    Client = Payload#payload_reply.client,
                    Value = Payload#payload_reply.value,
                    riak_core_vnode:reply(Client, {ok, {Value, 0}});
                false ->
                    noop
            end,
            true;
        update ->
            case ets:lookup(Ops, Label) of
                [{Label, Value}] ->
                    ok = execute_operation(Label, Value),
                    true = ets:delete(Ops, Label),
                    true;
                [] ->
                    lager:info("Operation not received for label: ~p", [Label]),
                    false
            end
    end.

is_sent_to_me(Destination, MyId) ->
    case Destination of
        all -> true;
        MyId -> true;
        _ -> false
    end.

-ifdef(TEST).

-endif.
