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

-record(state, {pendings,
                vclock :: dict(),
                idle,
                myid}).
               
reg_name(MyId) ->  list_to_atom(integer_to_list(MyId) ++ atom_to_list(?MODULE)). 

start_link(MyId, NLeaves) ->
    gen_server:start({global, reg_name(MyId)}, ?MODULE, [MyId, NLeaves], []).

handle(MyId, Message) ->
    %lager:info("Message received: ~p", [Message]),
    gen_server:cast({global, reg_name(MyId)}, Message).

label(MyId, Label) ->
    gen_server:cast({global, reg_name(MyId)}, {label, Label}).

clean_state(MyId) ->
    gen_server:call({global, reg_name(MyId)}, clean_state, infinity).

heartbeat(MyId, Time, Sender) ->
    gen_server:cast({global, reg_name(MyId)}, {heartbeat, Time, Sender}).

init([MyId, NLeaves]) ->
    Idle0 = lists:seq(0, NLeaves - 1),
    Idle1 = lists:delete(MyId, Idle0),
    lager:info("I am ~p, and my vector contains ~p",[MyId, Idle1]),
    VClock = lists:foldl(fun(Id, Acc) ->
                            dict:store(Id, 0, Acc)
                         end, dict:new(), Idle1),
    Pendings1 = lists:foldl(fun(Entry, Acc) ->
                                Name = list_to_atom(integer_to_list(Entry) ++  atom_to_list(eunomiakv_pops) ++ integer_to_list(MyId)),
                                dict:store(Entry, ets_queue:new(Name), Acc)
                            end, dict:new(), Idle1),
    erlang:send_after(10, self(), deliver),
    {ok, #state{pendings=Pendings1,
                myid=MyId,
                idle=Idle1,
                vclock=VClock}}.

handle_call(clean_state, _From, S0=#state{pendings=Pendings0, vclock=VClock0}) ->
    Pendings1 = lists:foldl(fun({Entry, Queue}, Acc) ->
                                dict:store(Entry, ets_queue:clean(Queue), Acc)
                            end, dict:new(), dict:to_list(Pendings0)),
    VClock1 = lists:foldl(fun(Id, Acc) ->
                            dict:store(Id, 0, Acc)
                          end, dict:new(), dict:fetch_keys(VClock0)),
    Idle1 = dict:fetch_keys(VClock0),
    {reply, ok, S0#state{pendings=Pendings1, vclock=VClock1, idle=Idle1}}.

handle_cast({completed, Sender, Clock}, S0=#state{vclock=VClock0, idle=Idle0}) ->
    VClock1 = dict:store(Sender, Clock, VClock0),
    {noreply, S0#state{vclock=VClock1, idle=[Sender|Idle0]}};


handle_cast({heartbeat, Time, Sender}, S0=#state{pendings=Pendings0}) ->
    %lager:info("Received heartbeat from ~p with clock ~p", [Sender, Time]),
    Queue0 = dict:fetch(Sender, Pendings0),
    Queue1 = ets_queue:in({Time, Sender, heartbeat}, Queue0),
    Pendings1 = dict:store(Sender, Queue1, Pendings0),
    {noreply, S0#state{pendings=Pendings1}};
    
handle_cast({label, Label}, S0=#state{pendings=Pendings0}) ->
    TimeStamp = Label#label.timestamp,
    Sender = Label#label.sender,
    Queue0 = dict:fetch(Sender, Pendings0),
    Queue1 = ets_queue:in({TimeStamp, Sender, Label}, Queue0),
    Pendings1 = dict:store(Sender, Queue1, Pendings0),
    {noreply, S0#state{
                       %vclock=VClock1i,
                        pendings=Pendings1}};

handle_cast(_Info, State) ->
    {noreply, State}.

handle_info(deliver, S0=#state{pendings=Pendings0, vclock=VClock0, idle=Idle0}) ->
    {Idle1, VClock1, Pendings1} = lists:foldl(fun(Entry, {I, V0, P0}) ->
                                                Q0 = dict:fetch(Entry, Pendings0),
                                                case deliver_labels(Q0, V0) of
                                                    {ok, {false, V1, Q1}} ->
                                                        P1 = dict:store(Entry, Q1, P0),
                                                        {I, V1, P1};
                                                    {ok, {true, V1, Q1}} ->
                                                        P1 = dict:store(Entry, Q1, P0),
                                                        {[Entry|I], V1, P1}
                                                end
                                              end, {[], VClock0, Pendings0}, Idle0),
    lager:info("stable time ~p",[compute_stable(VClock1)]),
    erlang:send_after(?STABILIZATION_FREQ_CONVERGER, self(), deliver),
    {noreply, S0#state{vclock=VClock1, idle=Idle1, pendings=Pendings1}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

compute_stable(Dict) ->
    List = [Value || {_, Value} <- dict:to_list(Dict)],
    lists:min(List).

deliver_labels(Queue, VClock0) ->
    case ets_queue:peek(Queue) of
        empty ->
            {ok, {true, VClock0, Queue}};
        {value, {TimeStamp, Sender, heartbeat}}  -> 
            VClock1 = dict:store(Sender, TimeStamp, VClock0),
            {{value, _}, Queue1} = ets_queue:out(Queue),
            deliver_labels(Queue1, VClock1);
        {value, {TimeStamp, Sender, Label}} ->
            case stable(dict:to_list(VClock0), TimeStamp, Sender) of
                true ->
                    {{value, _}, Queue1} = ets_queue:out(Queue),
                    case Label#label.operation of
                        remote_read ->
                            BKey = Label#label.bkey,
                            DocIdx = riak_core_util:chash_key(BKey),
                            PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
                            [{IndexNode, _Type}] = PrefList,
                            saturn_proxy_vnode:remote_read(IndexNode, Label),
                            Clock = dict:fetch(Sender, TimeStamp),
                            VClock1 = dict:store(Sender, Clock, VClock0),
                            deliver_labels(Queue1, VClock1);
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
                            Clock = dict:fetch(Sender, TimeStamp),
                            VClock1 = dict:store(Sender, Clock, VClock0),
                            deliver_labels(Queue1, VClock1);
                        update ->
                            BKey = Label#label.bkey,
                            Clock = Label#label.timestamp,
                            Node = Label#label.node,
                            Sender = Label#label.sender,
                            DocIdx = riak_core_util:chash_key(BKey),
                            PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
                            [{IndexNode, _Type}] = PrefList,
                            saturn_proxy_vnode:propagate(IndexNode, Clock, Node, Sender),
                            lager:info("Sent update with ts: ~p",[Clock]),
                            {ok, {false, VClock0, Queue1}}
                    end;
                false ->
                    {ok, {true, VClock0, Queue}}
            end
    end.

stable([], _TimeStamp, _Sender) ->
    true;

stable([{Sender, _Clock}|Rest], TimeStamp, Sender) ->
    stable(Rest, TimeStamp, Sender);

stable([{Entry, Stable}|Rest], TimeStamp, Sender) ->
    Clock =  dict:fetch(Entry, TimeStamp),
    case Stable >= Clock of
        true ->
            stable(Rest, TimeStamp, Sender);
        false ->
            false
    end.
                         
-ifdef(TEST).

-endif.
