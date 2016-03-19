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
-module(saturn_leaf_receiver).
-behaviour(gen_server).

-include("saturn_leaf.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).
-export([get_receivers/1,
         assign_convergers/1]).

-record(state, {nodes,
                scattered_receivers,
                myid}).
               
reg_name(MyId) ->  list_to_atom(integer_to_list(MyId) ++ atom_to_list(?MODULE)). 

start_link(MyId) ->
    gen_server:start({global, reg_name(MyId)}, ?MODULE, [MyId], []).

get_receivers(MyId) ->
    gen_server:call({global, reg_name(MyId)}, get_receivers, infinity).

assign_convergers(MyId) ->
    gen_server:call({global, reg_name(MyId)}, assign_convergers, infinity).

init([MyId]) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    GrossPrefLists = riak_core_ring:all_preflists(Ring, 1),
    ZeroPreflist = lists:foldl(fun(PrefList, Acc) ->
                                case hd(PrefList) of
                                    {0, _Node} ->
                                        hd(PrefList);
                                    {_OtherPartition, _Node} ->
                                        Acc
                                end
                               end, not_found, GrossPrefLists),
    Nodes = riak_core_ring:all_members(Ring),
    Convergers = [list_to_atom(atom_to_list(Node) ++ atom_to_list(saturn_leaf_converger)) || Node <- Nodes],
    case ZeroPreflist of
        not_found ->
            lager:error("Zero preflist not found", []);
        _ ->
            lists:foreach(fun(Name) ->
                            ok = saturn_leaf_converger:set_zeropl(Name, ZeroPreflist)
                          end, Convergers)
    end,
    {ok, #state{myid=MyId, nodes=Convergers}}.

handle_call(assign_convergers, _From, S0=#state{myid=MyId}) ->
    case groups_manager_serv:get_all_nodes() of
        {ok, []} ->
            lager:error("No no other nodes information", []),
            {reply, ok, S0};
        {ok, Group0} ->
            Group1 = lists:delete(MyId, Group0),
            Convergers1 = lists:foldl(fun(Id, Acc) ->
                            {ok, Receivers} = saturn_leaf_receiver:get_receivers(Id),
                            dict:store(Id, Receivers, Acc)
                          end, dict:new(), Group1),
            {ok, Ring} = riak_core_ring_manager:get_my_ring(),
            GrossPrefLists = riak_core_ring:all_preflists(Ring, 1),
            Partitions = [Partition || [{Partition, _Node}] <- GrossPrefLists],
            {Dict, _} = lists:foldl(fun(PrefList, {Acc, N}) ->
                                        D = lists:foldl(fun(Id, D0) ->
                                                            ConvId = dict:fetch(Id, Convergers1),
                                                            Entry = (N rem length(ConvId)) + 1,
                                                            dict:store(Id, lists:nth(Entry, ConvId), D0)
                                                        end, dict:new(), dict:fetch_keys(Convergers1)),
                                        ok = saturn_proxy_vnode:set_receivers(hd(PrefList), D),
                                        ok = saturn_proxy_vnode:init_vv(hd(PrefList), Group0, Partitions, MyId),
                                        saturn_proxy_vnode:send_heartbeat(hd(PrefList)),
                                        saturn_proxy_vnode:compute_times(hd(PrefList)),
                                        {dict:store(hd(PrefList), D, Acc), N+1}
                                    end, {dict:new(), 1}, GrossPrefLists),
            {reply, ok, S0#state{scattered_receivers=Dict}}
    end;

handle_call(get_receivers, _From, S0=#state{nodes=Nodes}) ->
    {reply, {ok, Nodes}, S0}.

handle_cast(_Info, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

-ifdef(TEST).

-endif.
