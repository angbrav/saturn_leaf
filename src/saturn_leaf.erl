-module(saturn_leaf).
-include("saturn_leaf.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         ping/0,
         update/3,
         read/2,
         async_read/3,
         async_txread/3,
         async_txwrite/3,
         async_update/4,
         clean/1,
         collect_stats/2,
         collect_stats_arrival/3,
         collect_stats_internal/3,
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

async_txread([{Bucket, Key}|_Rest]=BKeys, Clock, Client) ->
    DocIdx = riak_core_util:chash_key({Bucket, Key}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
    [{IndexNode, _Type}] = PrefList,
    saturn_proxy_vnode:async_txread(IndexNode, BKeys, Clock, Client).

async_txwrite([Pair|_Rest]=Pairs, Clock, Client) ->
    {{Bucket, Key}, _Value} = Pair,
    DocIdx = riak_core_util:chash_key({Bucket, Key}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
    [{IndexNode, _Type}] = PrefList,
    saturn_proxy_vnode:async_txwrite(IndexNode, Pairs, Clock, Client).

clean(MyId) ->
    ok = saturn_leaf_producer:clean_state(MyId), 
    ok = saturn_leaf_converger:clean_state(MyId), 
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    GrossPrefLists = riak_core_ring:all_preflists(Ring, 1),
    lists:foreach(fun(PrefList) ->
                    ok = saturn_proxy_vnode:clean_state(hd(PrefList))
                  end, GrossPrefLists),
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

collect_stats_arrival(To, From, Type) ->
    {ok, StatsRaw} = saturn_leaf_converger:collect_stats(To, From, Type),
    FinalStats = ?STALENESS:compute_cdf_from_orddict(StatsRaw),
    {ok, FinalStats}.

collect_stats_internal(To, From, Type) ->
    {ok, StatsRaw} = saturn_internal_serv:collect_stats(To, From, Type),
    FinalStats = ?STALENESS:compute_cdf_from_orddict(StatsRaw),
    {ok, FinalStats}.

spawn_wrapper(Module, Function, Pid, Args) ->
    Result = apply(Module, Function, Args),
    Pid ! Result.
