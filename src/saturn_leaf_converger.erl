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
                min_pending,
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
                min_pending=[infinity],
                myid=MyId}}.

handle_call(clean_state, _From, S0=#state{labels_queue=Labels}) ->
    {reply, ok, S0#state{labels_queue=ets_queue:clean(Labels), min_pending=[infinity]}}.

handle_cast({completed, TimeStamp}, S0=#state{labels_queue=Labels0, min_pending=[OldH|_]=Min0}) ->
    [NewH|_] = Min1 = lists:delete(TimeStamp, Min0),
    case (NewH>OldH) of
        true ->
            {Min2, Labels1} = dealwith_pending(ets_queue:peek(Labels0), Labels0, Min1),
            {noreply, S0#state{min_pending=Min2, labels_queue=Labels1}};
        false ->
            {noreply, S0#state{min_pending=Min1}}
    end;

handle_cast({new_stream, Stream, _SenderId}, S0=#state{labels_queue=Labels0, min_pending=Min0}) ->
    case ets_queue:is_empty(Labels0) of
        true ->
            {Min1, Stream1} = dealwith_stream(Stream, Min0),
            Labels1 = lists:foldl(fun(Label, Queue) ->
                                    ets_queue:in(Label, Queue)
                                  end, Labels0, Stream1),
            {noreply, S0#state{labels_queue=Labels1, min_pending=Min1}};
        false ->
            Labels1 = lists:foldl(fun(Label, Queue) ->
                                    ets_queue:in(Label, Queue)
                                  end, Labels0, Stream),
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

dealwith_pending(empty, Labels, Min) ->
    {Min, Labels};

dealwith_pending({value, Label}, Labels0, [Min|_]=ListMin) ->
    TimeStamp = Label#label.timestamp,
    case (TimeStamp =< Min) of
        true ->
            {{value, Label}, Labels1} = ets_queue:out(Labels0),
            case handle_label(Label) of
                true ->
                    dealwith_pending(ets_queue:peek(Labels1), Labels1, [TimeStamp|ListMin]);
                false ->
                    dealwith_pending(ets_queue:peek(Labels1), Labels1, ListMin)
            end;
        false ->
            {ListMin, Labels0}
    end.

dealwith_stream([], Min) ->
    {Min, []};

dealwith_stream([Label|Rest]=Labels, [Min|_]=ListMin) ->
    TimeStamp = Label#label.timestamp,
    case (TimeStamp =< Min) of
        true ->
            case handle_label(Label) of
                true ->
                    dealwith_stream(Rest, [TimeStamp|ListMin]);
                false ->
                    dealwith_stream(Rest, ListMin)
            end;
        false ->
            {ListMin, Labels}
    end.

handle_label(Label) ->
    case Label#label.operation of
        remote_read ->
            BKey = Label#label.bkey,
            DocIdx = riak_core_util:chash_key(BKey),
            PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
            [{IndexNode, _Type}] = PrefList,
            saturn_proxy_vnode:remote_read(IndexNode, Label),
            false;
        remote_reply ->
            Payload = Label#label.payload,
            Client = Payload#payload_reply.client,
            Value = Payload#payload_reply.value,
            case Payload#payload_reply.type_call of
                sync ->
                    riak_core_vnode:reply(Client, {ok, {Value, 0}});
                async ->
                    gen_server:reply(Client, {ok, {Value, 0}});
                tx ->
                    BKey = Payload#payload_reply.bkey,
                    gen_fsm:send_event(Client, {new_value, BKey, Value})
            end,
            false;
        update ->
            BKey = Label#label.bkey,
            Clock = Label#label.timestamp,
            Node = Label#label.node,
            Sender = Label#label.sender,
            DocIdx = riak_core_util:chash_key(BKey),
            PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
            [{IndexNode, _Type}] = PrefList,
            saturn_proxy_vnode:propagate(IndexNode, BKey, Clock, Sender, Node),
            true;
        write_tx ->
            [BKey|_Tail] = BKeys = Label#label.bkey,
            Clock = Label#label.timestamp,
            Node = Label#label.node,
            DocIdx = riak_core_util:chash_key(BKey),
            PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
            [{IndexNode, _Type}] = PrefList,
            saturn_proxy_vnode:remote_write_tx(IndexNode, Node, BKeys, Clock),
            true
    end.

-ifdef(TEST).

-endif.
