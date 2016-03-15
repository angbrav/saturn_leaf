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
-export([local_update/4,
         remote_read/4,
         remote_reply/4,
         what_you_know/2,
         pending_invalidations/3,
         propagation_finished/2]).

-record(state, {lamport_clock,
                precise_vv,
                matrix,
                busy,
                pending,
                log,
                rreads,
                myid}).
               
reg_name(MyId) ->  list_to_atom(integer_to_list(MyId) ++ atom_to_list(?MODULE)). 

start_link(MyId) ->
    gen_server:start({global, reg_name(MyId)}, ?MODULE, [MyId], []).

local_update(MyId, BKey, Value, Client) ->
    gen_server:cast({global, reg_name(MyId)}, {local_update, BKey, Value, Client}).

remote_read(MyId, BKey, RemoteId, Payload) ->
    gen_server:cast({global, reg_name(MyId)}, {handle_reads, remote_read, BKey, RemoteId, Payload}).

remote_reply(MyId, BKey, RemoteId, Payload) ->
    gen_server:cast({global, reg_name(MyId)}, {handle_reads, remote_reply, BKey, RemoteId, Payload}).

what_you_know(MyId, Fsm) ->
    gen_server:cast({global, reg_name(MyId)}, {what_you_know, Fsm}).

pending_invalidations(MyId, Fsm, Pending) ->
    gen_server:cast({global, reg_name(MyId)}, {pending_invalidations, Fsm, Pending}).

propagation_finished(MyId, Matrix) ->
    gen_server:cast({global, reg_name(MyId)}, {propagation_finished, Matrix}).

init([MyId]) ->
    {ok, Entries} = groups_manager_serv:get_all_nodes(), 
    PreciseVV = lists:foldl(fun(Entry, VV) ->
                                dict:store(Entry, 0, VV)
                            end, dict:new(), lists:delete(MyId, Entries)),
    {ok, #state{lamport_clock=0,
                precise_vv=PreciseVV,
                busy=false,
                pending=false,
                log=[],
                rreads=dict:new(),
                myid=MyId}}.

handle_cast({local_update, BKey, Value, Client}, S0=#state{lamport_clock=Clock0, myid=MyId, busy=Busy0, rreads=RReads0, log=Log0}) ->
    Clock1 = Clock0 + 1,
    DocIdx = riak_core_util:chash_key(BKey),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
    [{IndexNode, _Type}] = PrefList,
    saturn_proxy_vnode:clock(IndexNode, BKey, Value, Clock1, Client),
    Log1 = orddict:append(Clock1, {update, BKey, MyId, null}, Log0),
    case Busy0 of
        true ->
            {noreply, S0#state{lamport_clock=Clock1, log=Log1, pending=true}};
        false ->
            practi_propagation_fsm_sup:start_fsm([MyId, Log1, RReads0]),
            {noreply, S0#state{lamport_clock=Clock1, busy=true, log=[], rreads=dict:new()}}
    end;

handle_cast({handle_reads, Type, BKey, RemoteId, Payload}, S0=#state{lamport_clock=Clock0, myid=MyId, busy=Busy0, rreads=RReads0, log=Log0}) ->
    Clock1 = Clock0 + 1,
    RReads1 = dict:append(RemoteId, {Type, Clock1, BKey, Payload}, RReads0),
    case Busy0 of
        true ->
            {noreply, S0#state{lamport_clock=Clock1, rreads=RReads1, pending=true}};
        false ->
            practi_propagation_fsm_sup:start_fsm([MyId, Log0, RReads1]),
            {noreply, S0#state{lamport_clock=Clock1, busy=true, log=[], rreads=dict:new()}}
    end;

handle_cast({what_you_know, Fsm}, S0=#state{precise_vv=PreciseVV, myid=MyId}) ->
    gen_fsm:send_event(Fsm, {what_i_know, PreciseVV, MyId}),
    {noreply, S0};

handle_cast({pending_invalidations, Fsm, Invalidations0}, S0=#state{precise_vv=PreciseVV0, log=Log0, myid=MyId, lamport_clock=Clock0}) ->
    gen_fsm:send_event(Fsm, {pending_received, MyId}),
    Min = practi_vv:min_vclock(PreciseVV0),
    Max = practi_vv:max_vclock(PreciseVV0),
    Invalidations1 = case saturn_utilities:binary_search_sublist_log(Invalidations0, Min) of
            {ok, not_found, 0} ->
                Invalidations0;
            {ok, not_found, Index} ->
                lists:sublist(Invalidations0, Index+1, length(Invalidations0)-Index);
            {ok, found, Index} ->
                lists:sublist(Invalidations0, Index+1, length(Invalidations0)-Index)
          end,
    lager:info("Pending invalidations ~p", [Invalidations1]),
    {Invalidations2, PreciseVV1} = process_pending(Invalidations1, [], PreciseVV0, Max),
    NewMax = practi_vv:max_vclock(PreciseVV1),
    Clock1 = max(NewMax, Clock0),
    %% Maybe only stored the ones others have not seen?
    Log1 = saturn_utilities:merge_sorted_lists(Log0, Invalidations2, []),
    {noreply, S0#state{log=Log1, precise_vv=PreciseVV1, lamport_clock=Clock1}};

handle_cast({propagation_finished, Matrix}, S0=#state{log=Log0, myid=MyId, pending=Pending, rreads=RReads0}) ->
    case Pending of
        true ->
            practi_propagation_fsm_sup:start_fsm([MyId, Log0, RReads0]),
            {noreply, S0#state{log=[], pending=false, rreads=dict:new(), matrix=Matrix}};
        false ->
            {noreply, S0#state{busy=false, matrix=Matrix}}
    end;

handle_cast(Info, State) ->
    lager:error("Unexpedted cast: ~p", [Info]),
    {noreply, State}.

handle_call(Info, From, State) ->
    lager:error("Unexpedted call: ~p from ~p", [Info, From]),
    {reply, error, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

process_pending([], Log, PreciseVV0, _Max) ->
    {Log, PreciseVV0};

process_pending([Next|Rest], Log, PreciseVV0, Max) ->
    {Clock, Invalidations0} = Next,
    case Clock > Max of
        true ->
            {Invalidations1, PreciseVV1} = lists:foldl(fun({_Type, _BKey, Node, _Payload}=Inv, {I, VV0}) ->
                                                        VV1 = dict:store(Node, Clock, VV0),
                                                        case handle_invalidation(Inv, Clock) of
                                                            {ok, add} ->
                                                                {I ++ [Inv], VV1};
                                                            {ok, filter} ->
                                                                {I, VV1}
                                                        end
                                                       end, {[], PreciseVV0}, Invalidations0);
        false -> 
            {Invalidations1, PreciseVV1} = lists:foldl(fun({_Type, _BKey, Node, _Payload}=Inv, {I, VV0}) ->
                                                        ClockVector = dict:fetch(Node, VV0),
                                                        case Clock > ClockVector of
                                                            true ->
                                                                VV1 = dict:store(Node, Clock, VV0),
                                                                case handle_invalidation(Inv, Clock) of
                                                                    {ok, add} ->
                                                                        {I ++ [Inv], VV1};
                                                                    {ok, filter} ->
                                                                        {I, VV1}
                                                                end;
                                                            false ->
                                                                {I, VV0}
                                                        end
                                                       end, {[], PreciseVV0}, Invalidations0)
    end,
    case Invalidations1 of
        [] ->
            process_pending(Rest, Log, PreciseVV1, Max);
        _ ->
            process_pending(Rest, Log ++ [{Clock, Invalidations1}], PreciseVV1, Max)
    end.

handle_invalidation({Type, BKey, Node, Payload}, Clock) ->
    DocIdx = riak_core_util:chash_key(BKey),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
    [{IndexNode, _Type}] = PrefList,
    case Type of
        update ->
            ok = saturn_proxy_vnode:new_invalidation(IndexNode, Clock, BKey, Node),
            {ok, add};
        remote_read ->
            ok = saturn_proxy_vnode:remote_read(IndexNode, BKey, Node, Payload),
            {ok, filter};
        remote_reply ->
            {Client, Value} = Payload,
            riak_core_vnode:reply(Client, {ok, Value}),
            {ok, filter}
    end.
    
-ifdef(TEST).

-endif.
