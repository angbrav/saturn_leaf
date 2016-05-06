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
         clean_state/1,
         flush_list/4,
         dump_staleness/1,
         flush_queue/5]).

-record(state, {labels_queue :: queue(),
                ops,
                queue_len,
                staleness,
                myid}).
               
reg_name(MyId) ->  list_to_atom(integer_to_list(MyId) ++ atom_to_list(?MODULE)). 

start_link(MyId) ->
    gen_server:start({global, reg_name(MyId)}, ?MODULE, [MyId], []).

handle(MyId, Message) ->
    %lager:info("Message received: ~p", [Message]),
    gen_server:cast({global, reg_name(MyId)}, Message).

clean_state(MyId) ->
    gen_server:call({global, reg_name(MyId)}, clean_state, infinity).

dump_staleness(MyId) ->
    gen_server:call({global, reg_name(MyId)}, dump_staleness, infinity).

init([MyId]) ->
    Ops = ets:new(operations_converger, [set, named_table, private]),
    Staleness = ets:new(staleness, [set, named_table, private]),
    {ok, #state{labels_queue=queue:new(),
                ops=Ops,
                queue_len=0,
                staleness=Staleness,
                myid=MyId}}.

handle_call(clean_state, _From, S0=#state{ops=Ops, staleness=Staleness}) ->
    true = ets:delete(Ops),
    Ops1 = ets:new(operations_converger, [set, named_table, private]),
    true = ets:delete(Staleness),
    Staleness1 = ets:new(staleness, [set, named_table, private]),
    {reply, ok, S0#state{ops=Ops1, labels_queue=queue:new(), queue_len=0, staleness=Staleness1}};

handle_call(dump_staleness, _From, S0=#state{staleness=Staleness, myid=MyId}) ->
    ok = ets:tab2file(Staleness, list_to_atom(integer_to_list(MyId) ++ atom_to_list('-staleness.txt'))),
    {reply, ok, S0}.

handle_cast({new_stream, Stream, _SenderId}, S0=#state{labels_queue=Labels0, queue_len=QL0, ops=Ops, myid=MyId, staleness=Staleness}) ->
    %lager:info("New stream received. Label: ~p", Stream),
    case QL0 of
        0 ->
            {Labels1, QL1} = flush_list(Stream, Ops, MyId, Staleness),
            {noreply, S0#state{labels_queue=Labels1, queue_len=QL1}}; 
        _ ->
            {Labels1, Total} = lists:foldl(fun(Elem, {Queue, Sum}) ->
                                            {queue:in(Elem, Queue), Sum + 1}
                                           end, {Labels0, 0}, Stream),
            {noreply, S0#state{labels_queue=Labels1, queue_len=QL0+Total}}
    end;
    %{noreply, S0};

handle_cast({new_operation, Label, Value}, S0=#state{labels_queue=Labels0, ops=Ops, queue_len=QL0, myid=MyId, staleness=Staleness}) ->
    %lager:info("New operation received. Label: ~p", [Label]),
    case queue:peek(Labels0) of
        {value, Label} ->
            %true = ets:insert(Staleness, {Label, {MyId, saturn_utilities:now_microsec()}}),
            ok = execute_operation(Label, Value),
            Labels1 = queue:drop(Labels0),
            {Labels2, QL1} = flush_queue(Labels1, QL0-1, Ops, MyId, Staleness),
            {noreply, S0#state{labels_queue=Labels2, queue_len=QL1}};
        _ ->
            true = ets:insert(Ops, {Label, Value}),
            {noreply, S0}
    end;

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
    ok = saturn_proxy_vnode:propagate(IndexNode, BKey, Value, Clock).

flush_list([], _Ops, _MyId, _Staleness) ->
    {queue:new(), 0};

flush_list([Label|Rest]=List, Ops, MyId, Staleness) ->
    case handle_label(Label, Ops, MyId, Staleness) of
        true ->
            flush_list(Rest, Ops, MyId, Staleness);
        false ->
            {queue:from_list(List), length(List)}
    end.

flush_queue(Queue, Length, Ops, MyId, Staleness) ->
    case queue:peek(Queue) of
        {value, Label} ->
            case handle_label(Label, Ops, MyId, Staleness) of
                true ->
                    flush_queue(queue:drop(Queue), Length - 1, Ops, MyId, Staleness);
                false ->
                    {Queue, Length}
            end;
        _ ->
            {queue:new(), 0}
    end.

handle_label(Label, Ops, MyId, Staleness) ->
    case Label#label.operation of
        remote_read ->
            BKey = Label#label.bkey,
            DocIdx = riak_core_util:chash_key(BKey),
            PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
            true = ets:insert(Staleness, {Label, {MyId, saturn_utilities:now_microsec()}}),
            [{IndexNode, _Type}] = PrefList,
            saturn_proxy_vnode:remote_read(IndexNode, Label),
            true;
        remote_reply ->
            Payload = Label#label.payload,
            Client = Payload#payload_reply.client,
            Value = Payload#payload_reply.value,
            case Payload#payload_reply.type_call of
                sync ->
                    riak_core_vnode:reply(Client, {ok, {Value, 0}});
                async ->
                    gen_server:reply(Client, {ok, {Value, 0}})
            end,
            true;
        update ->
            case ets:lookup(Ops, Label) of
                [{Label, Value}] ->
                    true = ets:insert(Staleness, {Label, {MyId, saturn_utilities:now_microsec()}}),
                    ok = execute_operation(Label, Value),
                    true = ets:delete(Ops, Label),
                    true;
                [] ->
                    %lager:info("Operation not received for label: ~p", [Label]),
                    false
            end
    end.

-ifdef(TEST).

-endif.
