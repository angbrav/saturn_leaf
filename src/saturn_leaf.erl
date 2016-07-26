-module(saturn_leaf).
-include("saturn_leaf.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         ping/0,
         async_read/3,
         async_update/4,
         update/3,
         read/2,
         clean/0,
         collect_stats/2,
         init_store/2,
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

clean() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    GrossPrefLists = riak_core_ring:all_preflists(Ring, 1),
    Nodes = riak_core_ring:all_members(Ring),
    lists:foreach(fun(Node) ->
                    ok = saturn_client_receiver:clean_state(Node)
                  end, Nodes),
    lists:foreach(fun(PrefList) ->
                    ok = saturn_proxy_vnode:clean_state(hd(PrefList))
                  end, GrossPrefLists),
    ok.

init_store(Buckets, NKeys) ->
    lists:foreach(fun(Bucket) ->
                    lists:foreach(fun(Key) ->
                                    DocIdx = riak_core_util:chash_key({Bucket, Key}),
                                    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
                                    [{IndexNode, _Type}] = PrefList,
                                    ok = saturn_proxy_vnode:init_update(IndexNode, {Bucket, Key})
                                  end, lists:seq(1, NKeys))
                  end, Buckets),
    ok.

collect_stats(From, Type) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    GrossPrefLists = riak_core_ring:all_preflists(Ring, 1),
    FinalStatsRaw = lists:foldl(fun(PrefList, Acc) ->
                                    {ok, Stats} = saturn_proxy_vnode:collect_stats(hd(PrefList), From, Type),
                                    ?STALENESS:merge_raw(Acc, Stats)
                                end, [], GrossPrefLists),
    FinalStats = ?STALENESS:compute_cdf_from_orddict(FinalStatsRaw),
    {ok, FinalStats}.

spawn_wrapper(Module, Function, Pid, Args) ->
    Result = apply(Module, Function, Args),
    Pid ! Result.
