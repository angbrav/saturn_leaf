-module(saturn_leaf).
-include("saturn_leaf.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         ping/0,
         async_read/3,
         async_update/4,
         update/3,
         read/2,
         spawn_wrapper/4
        ]).

%% Public API

%% @doc Pings a random vnode to make sure communication is functional
ping() ->
    DocIdx = riak_core_util:chash_key({<<"ping">>, term_to_binary(now())}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, ping, ?PROXY_MASTER).

update({Bucket, Key}, Value, Clock) ->
    DocIdx = riak_core_util:chash_key({Bucket, Key}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
    [{IndexNode, _Type}] = PrefList,
    saturn_proxy_vnode:update(IndexNode, {Bucket, Key}, Value, Clock).
    
read({Bucket, Key}, Clock) ->
    DocIdx = riak_core_util:chash_key({Bucket, Key}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
    [{IndexNode, _Type}] = PrefList,
    saturn_proxy_vnode:read(IndexNode, {Bucket, Key}, Clock).

async_update({Bucket, Key}, Value, Clock, Client) ->
    DocIdx = riak_core_util:chash_key({Bucket, Key}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
    [{IndexNode, _Type}] = PrefList,
    saturn_proxy_vnode:async_update(IndexNode, {Bucket, Key}, Value, Clock, Client).
    
async_read({Bucket, Key}, Clock, Client) ->
    DocIdx = riak_core_util:chash_key({Bucket, Key}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
    [{IndexNode, _Type}] = PrefList,
    saturn_proxy_vnode:async_read(IndexNode, {Bucket, Key}, Clock, Client).

spawn_wrapper(Module, Function, Pid, Args) ->
    Result = apply(Module, Function, Args),
    Pid ! Result.
