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
-module(saturn_client_receiver).
-behaviour(gen_server).

-include("saturn_leaf.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).
-export([handle/2,
         init_vv/2,
         clean_state/1,
         new_clock/3,
         set_tree/2,
         new_lst/2]).

-record(state, {vv_lst,
                siblings,
                gst0}).

reg_name() -> list_to_atom(atom_to_list(node()) ++ atom_to_list(?MODULE)). 

start_link() ->
    gen_server:start({local, ?MODULE}, ?MODULE, [], []).

handle(read, [BKey, Clock]) ->
    gen_server:call({local, ?MODULE}, {read, BKey, Clock}, infinity);

handle(update, [BKey, Value, Clock]) ->
    gen_server:call({local, ?MODULE}, {update, BKey, Value, Clock}, infinity).

init_vv(Node, Entries) ->
    gen_server:call({?MODULE, Node}, {init_vv, Entries}, infinity).

set_tree(Node, Entries) ->
    gen_server:call({?MODULE, Node}, {set_tree, Entries}, infinity).

clean_state(Node) ->
    gen_server:call({?MODULE, Node}, clean_state, infinity).

new_clock(Node, Partition, Clock) ->
    gen_server:cast({?MODULE, Node}, {new_clock, Partition, Clock}).

new_lst(Node, LST) ->
    gen_server:cast({?MODULE, Node}, {new_lst, Node, LST}).

init([]) ->
    lager:info("Client receiver started at ~p", [reg_name()]),
    {ok, nostate}.

handle_call({init_vv, Entries}, _From, _S0) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    GrossPrefLists = riak_core_ring:all_preflists(Ring, 1),
    Nodes = riak_core_ring:all_members(Ring),
    VV = lists:foldl(fun(Entry, Acc) ->
                        dict:store(Entry, 0, Acc)
                     end, dict:new(), Entries),
    MyNode = node(),
    Siblings = lists:foldl(fun(Node, Acc) ->
                            case Node of
                                MyNode -> Acc;       
                                _ -> dict:store(Node, VV, Acc) 
                            end
                           end, dict:new(), Nodes),
    VV_LST = lists:foldl(fun(Preflist, Acc) ->
                            {Partition, _Node} = hd(Preflist),
                            dict:store(Partition, VV, Acc)
                         end, dict:new(), GrossPrefLists),
    GST0 = lists:foldl(fun(Id, Acc) ->
                        dict:store(Id, infinity, Acc)
                       end, dict:new(), Entries),
    {reply, ok, #state{siblings=Siblings, vv_lst=VV_LST, gst0=GST0}};

handle_call({set_tree, Entries}, _From, S0=#state{vv_lst=VV_LST0}) ->
    VV = lists:foldl(fun(Entry, Acc) ->
                        dict:store(Entry, 0, Acc)
                     end, dict:new(), Entries),
    VV_LST = lists:foldl(fun(Partition, Acc) ->
                            dict:store(Partition, VV, Acc)
                         end, dict:new(), dict:fetch_keys(VV_LST0)),
    GST0 = lists:foldl(fun(Id, Acc) ->
                        dict:store(Id, infinity, Acc)
                       end, dict:new(), Entries),
    {reply, ok, S0#state{vv_lst=VV_LST, gst0=GST0}};

handle_call(clean_state, _From, S0=#state{siblings=Siblings, vv_lst=VV_LST, gst0=GST0}) ->
    VV = clean_vector(GST0),
    {reply, ok, S0#state{siblings=clean_vector(Siblings),
                         vv_lst=clean_lst_vector(VV_LST, VV)}};

handle_call({read, BKey, Clock}, From, S0) ->
    saturn_leaf:async_read(BKey, Clock, From),
    {noreply, S0};

handle_call({update, BKey, Value, Clock}, From, S0) ->
    saturn_leaf:async_update(BKey, Value, Clock, From),
    {noreply, S0}.

handle_cast({new_clock, Partition, Clock}, S0=#state{vv_lst=VV_LST}) ->
    %lager:info("New clock: ~p", [{Partition, Clock}]),
    VV_LST1 = dict:store(Partition, Clock, VV_LST),
    {noreply, S0#state{vv_lst=VV_LST1}};

handle_cast({new_lst, Node, LST}, S0=#state{siblings=Siblings}) ->
    Siblings1 = dict:store(Node, LST, Siblings),
    {noreply, S0#state{siblings=Siblings1}};

handle_cast(_Info, State) ->
    {noreply, State}.

handle_info(compute_clocks, S0=#state{siblings=Siblings, vv_lst=VV_LST, gst0=GST0}) ->
    LocalGST = lists:foldl(fun({_Partition, LST}, Acc0) ->
                            lists:foldl(fun({Id, Clock}, Acc1) ->
                                            dict:store(Id, min(dict:fetch(Id, Acc1), Clock), Acc1)
                                        end, Acc0, dict:to_list(LST))
                            end, GST0, dict:to_list(VV_LST)),
    GST = lists:foldl(fun({_Node, LST}, Acc0) ->
                        lists:foldl(fun({Id, Clock}, Acc1) ->
                                        dict:store(Id, min(dict:fetch(Id, Acc1), Clock), Acc1)
                                    end, Acc0, dict:to_list(LST))
                      end, LocalGST, dict:to_list(Siblings)),
    MyNode = node(),
    lists:foreach(fun(Partition) ->
                    saturn_proxy_vnode:new_gst({Partition, MyNode}, GST)
                  end, dict:fetch_keys(VV_LST)),
    lists:foreach(fun(Node) ->
                    saturn_client_receiver:new_lst(Node, MyNode, LocalGST)
                  end, dict:fetch_keys(Siblings)),
    erlang:send_after(?TIMES_FREQ, self(), compute_clocks),
    {noreply, S0};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

clean_vector(Vector) ->
    lists:foldl(fun(Entry, Acc) ->
                    dict:store(Entry, 0, Acc)
                end, dict:new(), dict:fetch_keys(Vector)).

clean_lst_vector(Vector, Init) ->
    lists:foldl(fun(Entry, Acc) ->
                    dict:store(Entry, Init, Acc)
                end, dict:new(), dict:fetch_keys(Vector)).

-ifdef(TEST).

-endif.
