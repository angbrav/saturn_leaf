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
                ops_dict :: dict(),
                myid}).
               
reg_name(MyId) ->  list_to_atom(integer_to_list(MyId) ++ atom_to_list(?MODULE)). 

start_link(MyId) ->
    gen_server:start({global, reg_name(MyId)}, ?MODULE, [MyId], []).

handle(MyId, Message) ->
    lager:info("Message received: ~p", [Message]),
    gen_server:call({global, reg_name(MyId)}, Message, infinity).

init([MyId]) ->
    {ok, #state{labels_queue=queue:new(),
                ops_dict=dict:new(),
                myid=MyId}}.

handle_call({new_stream, Stream, _SenderId}, _From, S0=#state{labels_queue=Labels0, ops_dict=_Ops0}) ->
    lager:info("New stream received. Label: ~p", Stream),
    case queue:len(Labels0) of
        0 ->
            S1 = flush_queue(Stream, S0);
        _ ->
            Labels1 = queue:join(Labels0, queue:from_list(Stream)),
            S1 = S0#state{labels_queue=Labels1}
    end,
    {reply, ok, S1};

handle_call({new_operation, Label, Value}, _From, S0=#state{labels_queue=Labels0, ops_dict=Ops0}) ->
    lager:info("New operation received. Label: ~p", [Label]),
    case queue:peek(Labels0) of
        {value, Label} ->
            ok = execute_operation(Label, Value),
            Labels1 = queue:drop(Labels0),
            S1 = flush_queue(queue:to_list(Labels1), S0);
        _ ->
            Ops1 = dict:store(Label, Value, Ops0),
            S1 = S0#state{ops_dict=Ops1}
    end,
    {reply, ok, S1}.

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

flush_queue([], S0) ->
    S0#state{labels_queue=queue:new()};

flush_queue([Label|Rest]=Labels, S0=#state{ops_dict=Ops0, myid=MyId}) ->
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
            flush_queue(Rest, S0);
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
            flush_queue(Rest, S0);
        update ->
            case dict:find(Label, Ops0) of
                {ok, Value} ->
                    ok = execute_operation(Label, Value),
                    Ops1 = dict:erase(Label, Ops0),
                    flush_queue(Rest, S0#state{ops_dict=Ops1});
                _ ->
                    lager:info("Operation not received for label: ~p", [Label]),
                    S0#state{labels_queue=queue:from_list(Labels)}
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
