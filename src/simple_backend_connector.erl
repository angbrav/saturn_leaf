-module(simple_backend_connector).

-include("saturn_leaf.hrl").

-export([update/1,
         read/1,
         propagation/1
        ]).

update(Payload)->
    {Key, Value, TimeStamp, Seq} = Payload,
    DocIdx = riak_core_util:chash_key({?BUCKET, Key}),
    PrefListStore = riak_core_apl:get_primary_apl(DocIdx, 1, ?SIMPLE_SERVICE),
    PrefListProxy = riak_core_apl:get_primary_apl(DocIdx, 1, ?PROXY_SERVICE),
    [{IndexNodeStore, _TypeStore}] = PrefListStore,
    [{IndexNodeProxy, _TypeProxy}] = PrefListProxy,
    saturn_simple_backend_vnode:update(IndexNodeStore, Key, {Value, TimeStamp}).

read(Payload)->
    {Key, Client} = Payload,
    DocIdx = riak_core_util:chash_key({?BUCKET, Key}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?SIMPLE_SERVICE),
    [{IndexNode, _Type}] = PrefList,
    saturn_simple_backend_vnode:read(IndexNode, Key).

propagation(Payload)->
    {Key, Value, TimeStamp} = Payload,
    DocIdx = riak_core_util:chash_key({?BUCKET, Key}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?SIMPLE_SERVICE),
    [{IndexNode, _Type}] = PrefList,
    saturn_simple_backend_vnode:propagation(IndexNode, Key, {Value, TimeStamp}).
