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
-export([heartbeat/4,
         propagate/5,
         remote_read/5, 
         remote_reply/5]).

-record(state, {zeropl,
                myid}).
               
reg_name(MyId) ->  list_to_atom(integer_to_list(MyId) ++ atom_to_list(?MODULE)). 

start_link(MyId) ->
    gen_server:start({global, reg_name(MyId)}, ?MODULE, [MyId], []).

propagate(MyId, BKey, Value, TimeStamp, Sender) ->
    gen_server:cast({global, reg_name(MyId)}, {remote_update, BKey, Value, TimeStamp, Sender}).

remote_read(MyId, BKey, Sender, Clock, Client) ->
    gen_server:cast({global, reg_name(MyId)}, {remote_update, BKey, Sender, Clock, Client}).

remote_reply(MyId, BKey, Value, Client, Clock) ->
    gen_server:cast({global, reg_name(MyId)}, {remote_reply, BKey, Value, Client, Clock}).
 
heartbeat(MyId, Partition, Clock, Sender) ->
    gen_server:cast({global, reg_name(MyId)}, {heartbeat, Partition, Clock, Sender}).

init([MyId]) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    GrossPrefLists = riak_core_ring:all_preflists(Ring, 1),
    Partitions = [Partition || {Partition, _Node} <- GrossPrefLists],
    {ok, Entries} = groups_manager_serv:get_all_nodes(), 
    ZeroPreflist = lists:foldl(fun(PrefList, Acc) ->
                                ok = saturn_proxy_vnode:init_vv(PrefList, Entries, Partitions, MyId),
                                saturn_proxy_vnode:send_heartbeat(PrefList),
                                saturn_proxy_vnode:compute_times(PrefList),
                                case hd(PrefList) of
                                    {0, _Node} ->
                                        PrefList;
                                    {_OtherPartition, _Node} ->
                                        Acc
                                end
                               end, not_found, GrossPrefLists),
    case ZeroPreflist of
        not_found ->
            lager:error("Zero preflist not found", []),
            {ok, #state{myid=MyId, zeropl=not_found}};
        _ ->
            {ok, #state{myid=MyId, zeropl=ZeroPreflist}}
    end.

handle_call(Info, From, State) ->
    lager:error("Unhandled message ~p, from ~p", [Info, From]),
    {reply, ok, State}.

handle_cast({remote_update, BKey, Sender, Clock, Client}, S0) ->
    DocIdx = riak_core_util:chash_key(BKey),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode:propagate(IndexNode, BKey, Sender, Clock, Client),
    {noreply, S0};

handle_cast({remote_reply, BKey, Value, Client, Clock}, S0) ->
    DocIdx = riak_core_util:chash_key(BKey),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode:propagate(IndexNode, Value, Client, Clock),
    {noreply, S0};

handle_cast({heartbeat, Partition, Clock, Sender}, S0=#state{zeropl=ZeroPreflist}) ->
    case Partition of
        0 ->
            IndexNode = ZeroPreflist;
        _ ->
            DocIdx = riak_core_util:chash_key(Partition - 1),
            PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
            [{IndexNode, _Type}] = PrefList
    end,
    riak_core_vnode:heartbeat(IndexNode, Clock, Sender),
    {noreply, S0};

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
