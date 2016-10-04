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

-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).
-export([handle/2,
         label/2,
         heartbeat/3,
         clean_state/1]).

-record(state, {labels,
                vclock :: dict(),
                busy,
                myid}).
               
reg_name(MyId) ->  list_to_atom(integer_to_list(MyId) ++ atom_to_list(?MODULE)). 

start_link(MyId, NLeaves) ->
    gen_server:start({global, reg_name(MyId)}, ?MODULE, [MyId, NLeaves], []).

handle(MyId, Message) ->
    %lager:info("Message received: ~p", [Message]),
    gen_server:cast({global, reg_name(MyId)}, Message).

label(MyId, Label) ->
    gen_server:cast({global, reg_name(MyId)}, {label, Label}).

heartbeat(MyId, Time, Sender) ->
    gen_server:cast({global, reg_name(MyId)}, {heartbeat, Time, Sender}).

clean_state(MyId) ->
    gen_server:call({global, reg_name(MyId)}, clean_state, infinity).

init([MyId, NLeaves]) ->
    VClock = lists:foldl(fun(Id, Acc) ->
                            dict:store(Id, 0, Acc)
                         end, dict:new(), lists:seq(0, NLeaves-1)),
    Labels = ets:new(labels_converger, [ordered_set, named_table, private]),
    erlang:send_after(10, self(), deliver),
    {ok, #state{labels=Labels,
                myid=MyId,
                busy=false,
                vclock=VClock}}.

handle_call(clean_state, _From, S0=#state{labels=Labels0, vclock=VClock0}) ->
    true = ets:delete(Labels0),
    Labels1 = ets:new(labels_converger, [ordered_set, named_table, private]),
    VClock1 = lists:foldl(fun(Id, Acc) ->
                            dict:store(Id, 0, Acc)
                          end, dict:new(), dict:fetch_keys(VClock0)),
    {reply, ok, S0#state{labels=Labels1, vclock=VClock1, busy=false}}.

handle_cast({heartbeat, Time, Sender}, S0=#state{vclock=VClock0}) ->
    VClock1 = dict:store(Sender, Time, VClock0),
    {noreply, S0#state{vclock=VClock1}};

handle_cast(completed, S0) ->
    {noreply, S0#state{busy=false}};
    
handle_cast({label, Label}, S0=#state{vclock=VClock0, labels=Labels}) ->
    TimeStamp = Label#label.timestamp,
    Node = Label#label.node,
    Sender = Label#label.sender,
    ets:insert(Labels, {{TimeStamp, Sender, Node, Label}, in}),
    VClock1 = dict:store(Sender, TimeStamp, VClock0),
    {noreply, S0#state{vclock=VClock1}};

handle_cast(_Info, State) ->
    {noreply, State}.

handle_info(deliver, S0=#state{labels=Labels, vclock=VClock0, busy=Busy}) ->
    StableTime1 = compute_stable(VClock0),
    case Busy of
        true ->
            erlang:send_after(?STABILIZATION_FREQ, self(), deliver),
            {noreply, S0};
        false ->
            {ok, Busy1} = deliver_labels(Labels, StableTime1),
            erlang:send_after(?STABILIZATION_FREQ, self(), deliver),
            {noreply, S0#state{busy=Busy1}}
    end;

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

compute_stable(VClock) ->
    lists:foldl(fun({_Id, Clock}, Min) ->
                    min(Clock, Min)
                 end, infinity, dict:to_list(VClock)).

deliver_labels(Labels, StableTime) ->
    case ets:first(Labels) of
        '$end_of_table' ->
            {ok, false};
        {TimeStamp, _Sender, _Node, Label}=Key when TimeStamp =< StableTime ->
            true = ets:delete(Labels, Key),
            case Label#label.operation of
                remote_read ->
                    BKey = Label#label.bkey,
                    DocIdx = riak_core_util:chash_key(BKey),
                    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
                    [{IndexNode, _Type}] = PrefList,
                    saturn_proxy_vnode:remote_read(IndexNode, Label),
                    deliver_labels(Labels, StableTime);
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
                    deliver_labels(Labels, StableTime);
                update ->
                    BKey = Label#label.bkey,
                    Clock = Label#label.timestamp,
                    Node = Label#label.node,
                    Sender = Label#label.sender,
                    DocIdx = riak_core_util:chash_key(BKey),
                    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
                    [{IndexNode, _Type}] = PrefList,
                    saturn_proxy_vnode:propagate(IndexNode, Clock, Node, Sender),
                    {ok, true}
            end;
        _Key ->
            {ok, false}
    end.

-ifdef(TEST).

-endif.
