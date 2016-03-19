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

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).
-export([heartbeat/4,
         propagate/5,
         remote_read/5, 
         set_zeropl/2,
         remote_reply/5]).

-record(state, {zeropl}).
               
start_link() ->
    Name = list_to_atom(atom_to_list(node()) ++ atom_to_list(?MODULE)),
    gen_server:start({global, Name}, ?MODULE, [], []).

propagate(Name, BKey, Value, TimeStamp, Sender) ->
    gen_server:cast({global, Name}, {remote_update, BKey, Value, TimeStamp, Sender}).

remote_read(Name, BKey, Sender, Clock, Client) ->
    gen_server:cast({global, Name}, {remote_read, BKey, Sender, Clock, Client}).

remote_reply(Name, BKey, Value, Client, Clock) ->
    gen_server:cast({global, Name}, {remote_reply, BKey, Value, Client, Clock}).
 
heartbeat(Name, Partition, Clock, Sender) ->
    gen_server:cast({global, Name}, {heartbeat, Partition, Clock, Sender}).

set_zeropl(Name, ZeroPreflist) ->
    gen_server:call({global, Name}, {set_zeropl, ZeroPreflist}, infinity).

init([]) ->
    {ok, #state{zeropl=not_found}}.

handle_call({set_zeropl, ZeroPreflist}, _From, S0) ->
    {reply, ok, S0#state{zeropl=ZeroPreflist}};

handle_call(Info, From, State) ->
    lager:error("Unhandled message ~p, from ~p", [Info, From]),
    {reply, ok, State}.

handle_cast({remote_update, BKey, Value, Clock, Sender}, S0) ->
    DocIdx = riak_core_util:chash_key(BKey),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
    [{IndexNode, _Type}] = PrefList,
    saturn_proxy_vnode:propagate(IndexNode, BKey, Value, Clock, Sender),
    {noreply, S0};

handle_cast({remote_read, BKey, Sender, Clock, Client}, S0) ->
    DocIdx = riak_core_util:chash_key(BKey),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
    [{IndexNode, _Type}] = PrefList,
    saturn_proxy_vnode:remote_read(IndexNode, BKey, Sender, Clock, Client),
    {noreply, S0};

handle_cast({remote_reply, BKey, Value, Client, Clock}, S0) ->
    DocIdx = riak_core_util:chash_key(BKey),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
    [{IndexNode, _Type}] = PrefList,
    saturn_proxy_vnode:remote_reply(IndexNode, Value, Client, Clock),
    {noreply, S0};

handle_cast({heartbeat, Partition, Clock, Sender}, S0=#state{zeropl=ZeroPreflist}) ->
    case Partition of
        0 ->
            IndexNode = ZeroPreflist;
        _ ->
            PrefList = riak_core_apl:get_primary_apl(Partition - 1, 1, ?PROXY_SERVICE),
            [{IndexNode, _Type}] = PrefList
    end,
    saturn_proxy_vnode:heartbeat(IndexNode, Clock, Sender),
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
