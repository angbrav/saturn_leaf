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
-export([handle/2,
         clean_state/1]).

-record(state, {labels_queue :: queue(),
                myid}).
               
reg_name(MyId) ->  list_to_atom(integer_to_list(MyId) ++ atom_to_list(?MODULE)). 

start_link(MyId) ->
    gen_server:start({global, reg_name(MyId)}, ?MODULE, [MyId], []).

handle(MyId, Message) ->
    %lager:info("Message received: ~p", [Message]),
    gen_server:cast({global, reg_name(MyId)}, Message).

clean_state(MyId) ->
    gen_server:call({global, reg_name(MyId)}, clean_state, infinity).

init([MyId]) ->
    Name = list_to_atom(integer_to_list(MyId) ++ "converger_queue"),
    {ok, #state{labels_queue=ets_queue:new(Name),
                myid=MyId}}.

handle_call(clean_state, _From, S0=#state{labels_queue=Labels}) ->
    {reply, ok, S0#state{labels_queue=ets_queue:clean(Labels)}}.

handle_cast(completed, S0=#state{labels_queue=Labels0}) ->
    {_, Labels1} = ets_queue:out(Labels0),
    Labels2 =  handle_label(ets_queue:peek(Labels1), Labels1),
    {noreply, S0#state{labels_queue=Labels2}};
    
handle_cast({new_stream, TaggedStream, _SenderId}, S0=#state{labels_queue=Labels0}) ->
    lager:info("New stream received from ~p: ~p [Tagged]", [_SenderId, TaggedStream]),
    Stream = lists:map(
        fun({Label,_}) -> Label end,
        TaggedStream),
    lager:info("New stream received from ~p: ~p", [_SenderId, Stream]),
    Empty = ets_queue:is_empty(Labels0),
    Labels1 = lists:foldl(fun(Label, Queue) ->
                            ets_queue:in(Label, Queue)
                          end, Labels0, Stream),
    case Empty of
        true ->
            Labels2 =  handle_label(ets_queue:peek(Labels1), Labels1),
            {noreply, S0#state{labels_queue=Labels2}};
        false ->
            {noreply, S0#state{labels_queue=Labels1}}
    end;

handle_cast(_Info, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_label(empty, Queue) ->
    Queue;

handle_label({value, Label}, Queue) ->
    case Label#label.operation of
        remote_read ->
            BKey = Label#label.bkey,
            DocIdx = riak_core_util:chash_key(BKey),
            PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
            [{IndexNode, _Type}] = PrefList,
            saturn_proxy_vnode:remote_read(IndexNode, Label),
            {_, Queue1} = ets_queue:out(Queue),
            handle_label(ets_queue:peek(Queue1), Queue1);
        remote_reply ->
            Payload = Label#label.payload,
            _Client = Payload#payload_reply.client,
            _Value = Payload#payload_reply.value,
            case Payload#payload_reply.type_call of
                sync ->
                    noop;
                    %riak_core_vnode:reply(Client, {ok, {Value, 0}});
                async ->
                    %gen_server:reply(Client, {ok, {Value, 0}})
                    noop
            end,
            {_, Queue1} = ets_queue:out(Queue),
            handle_label(ets_queue:peek(Queue1), Queue1);
        update ->
            BKey = Label#label.bkey,
            Clock = Label#label.timestamp,
            Node = Label#label.node,
            Sender = Label#label.sender,
            DocIdx = riak_core_util:chash_key(BKey),
            PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
            [{IndexNode, _Type}] = PrefList,
            saturn_proxy_vnode:propagate(IndexNode, Clock, Node, Sender),
            Queue
    end.

-ifdef(TEST).

-endif.
