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
-module(practi_propagation_fsm).
-behaviour(gen_fsm).

-export([start_link/3]).
        
-export([init/1,
         code_change/4,
         handle_event/3,
         handle_info/3,
         terminate/3,
         handle_sync_event/4]).
-export([gather_info/2,
         wait_for_reply/2,
         prune_log/2,
         stop/2
        ]).

-record(state, {server,
                log,
                matrix,
                left,
                group,
                reason,
                rreads
                }).

-define(CONNECT_TIMEOUT,20000).

%% ===================================================================
%% Public API
%% ===================================================================

start_link(Server, Log, RReads) ->
    gen_fsm:start_link(?MODULE, [Server, Log, RReads], []).

%% ===================================================================
%% gen_fsm callbacks
%% ===================================================================

init([Server, Log]) ->
    {ok, gather_info, #state{server=Server,
                             matrix=dict:new(),
                             reason=init,
                             log=Log}, 0}.

gather_info(timeout, S0) ->
    case groups_manager_serv:get_all_nodes_but_myself() of
        {ok, []} ->
            Group = [],
            lager:error("No no other nodes information", []);
        {ok, Group} ->
            lists:foreach(fun(Id) ->
                            saturn_leaf_converger:what_you_know(Id, self())
                          end, Group)
    end,
    {next_state, wait_for_reply, S0#state{group=Group, left=length(Group)},?CONNECT_TIMEOUT}.

get_pending([], _Receiver, Vector0, _Min, Invalidations0, _EntriesLeft1, RV0) ->
    Vector1 = lists:fold(fun(Entry, Acc) ->
                            NewClock = dict:fetch(Entry, RV0),
                            dict:store(Entry, NewClock, Acc)
                         end, Vector0, dict:fetch_keys(RV0)),
    {Invalidations0, Vector1};

get_pending([Next|Rest], Receiver, Vector0, Min, Invalidations0, EntriesLeft0, RV0) ->
    {Clock, List} = Next,
    case Clock =< Min of
        true ->
            Vector1 = lists:fold(fun(Entry, Acc) ->
                                    NewClock = dict:fetch(Entry, RV0),
                                    dict:store(Entry, NewClock, Acc)
                                 end, Vector0, dict:fetch_keys(RV0)),
            {Invalidations0, Vector1};
        false ->
            {Invalidations1, N} = lists:fold(fun({Type, BKey, Node}, {Acc, Nodes}) ->
                                                case Node of
                                                    Receiver ->
                                                        {Acc, Nodes};
                                                    _ ->
                                                        Entry = dict:fetch(Node, Vector0),
                                                        case Clock > Entry of
                                                        true ->
                                                            {dict:append(Clock, {Type, BKey, Node, null}, Acc), Nodes ++ [Node]};
                                                        false ->
                                                            {Acc, Nodes}
                                                        end
                                                end
                                             end, {Invalidations0, []}, List),
            case EntriesLeft0 of
                [] ->
                    get_pending(Rest, Receiver, Vector0, Min, Invalidations1, [], RV0);
                _ ->
                    {EntriesLeft1, RV1} = lists:foldl(fun(Entry, {Acc1, Acc2})->
                                                        case lists:member(Entry, Acc1) of
                                                            true ->
                                                                {lists:delete(Entry, Acc1), dict:store(Entry, Clock, Acc2)};
                                                            false ->
                                                                {Acc1, Acc2}
                                                        end
                                                     end, {EntriesLeft0, RV0}, N),
                    get_pending(Rest, Receiver, Vector0, Min, Invalidations1, EntriesLeft1, RV1)
            end
    end.

add_selective_events(Events, Pending0, Node) ->
    list:foldl(fun({Type, Clock, BKey, Payload}, Acc) ->
                orddict:append(Clock, {Type, BKey, Node, Payload}, Acc)
               end, Pending0, Events).    

wait_for_reply({what_i_know, Vector, Id}, S0=#state{matrix=Matrix0, log=Log0, rreads=RReads0, server=MyId})->
    Min = practi_vv:min_vclock(Vector),
    {Pending0, NewVector} = get_pending(lists:reverse(Log0), Id, Vector, Min, dict:new(), disct:fetch_keys(Vector), dict:new()),
    case dict:find(Id, RReads0) of
        {ok, List} ->
            Pending1 = add_selective_events(List, Pending0, MyId);
        error ->
            Pending1 = Pending0
    end,
    saturn_leaf_converger:pending_ops(Id, Pending1),
    Matrix1 = dict:store(Id, NewVector, Matrix0),
    {next_state, wait_for_reply, S0#state{matrix=Matrix1}, ?CONNECT_TIMEOUT};

wait_for_reply({pending_received, _Id}, S0=#state{left=Left0})->
    case Left0 of
        1 ->
            {next_state, stop, S0#state{left=0}, 0};
        _ ->
            {next_state, wait_for_reply, S0#state{left=Left0-1}, ?CONNECT_TIMEOUT}
    end;

wait_for_reply(timeout, State) ->
    %%TODO: Retry if needed
    lager:error("Timeout waiting for 'what_you_know' or 'pending_received'",[]),
    {next_state, stop, State#state{reason=timeout},0}.

prune_log(timeout, S0=#state{log=Log0, matrix=Matrix}) ->
    Entries = dict:fetch_keys(Matrix),
    VectorInit = practi_vv:init_max_vclock(Entries),
    MinVector = lists:foldl(fun(Entry, Acc1) ->
                                VClock = dict:fetch(Entry, Matrix),
                                practi_vv:min_between_vclocks(VClock, Acc1)
                            end, VectorInit, Entries),
    Min = practi_vv:min_vclock(MinVector),
    Max = practi_vv:max_vclock(MinVector),
    Log1 = case saturn_utilities:binary_search_sublist_log(Log0, Min) of
            {ok, not_found, 0} ->
                Log0;
            {ok, not_found, Index} ->
                lists:sublist(Log0, Index+1, length(Log0)-Index);
            {ok, found, Index} ->
                lists:sublist(Log0, Index+1, length(Log0)-Index)
          end,
    Log2 = prune(Log1, MinVector, Max, []),
    {next_state, stop, S0#state{log=Log2}}. 

prune([], _MinVector, _Max, NewLog) ->
    NewLog;

prune([Next, Rest], MinVector, Max, NewLog) ->
    {Clock, Invalidations0} = Next,
    case Clock > Max of
        true ->
            NewLog ++ [Rest];
        false ->
            Invalidations1 = lists:foldl(fun({_Key, Node}=I, Acc) ->
                                            ClockVector = dict:fetch(Node, MinVector),
                                            case ClockVector < Clock of
                                                true ->
                                                    Acc ++ [I];
                                                false ->
                                                    Acc
                                            end
                                         end, [], Invalidations0),  
            prune(Rest, MinVector, Max, NewLog ++ [Clock, Invalidations1])
    end.

stop(timeout, State=#state{reason=Reason, matrix=Matrix, server=Server}) ->
    saturn_leaf_converger:propagation_finished(Server, Matrix),
    {stop, Reason, State}.

handle_info(Message, _StateName, StateData) ->
    lager:error("Unexpected message: ~p",[Message]),
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_,_,_) -> ok.
