-module(simple_backend_connector).

-include("saturn_leaf.hrl").

-export([update/1,
         read/1,
         propagation/1
        ]).

update(Payload)->
    {Key, Value, TimeStamp} = Payload,
    IndexNode = get_indexnode(Key),
    saturn_simple_backend_vnode:update(IndexNode, Key, {Value, TimeStamp}).

read(Payload)->
    {Key} = Payload,
    IndexNode = get_indexnode(Key),
    saturn_simple_backend_vnode:read(IndexNode, Key).

propagation(Payload)->
    {Key, Value, TimeStamp} = Payload,
    IndexNode = get_indexnode(Key),
    saturn_simple_backend_vnode:propagation(IndexNode, Key, {Value, TimeStamp}).

get_indexnode(Key) ->
    DocIdx = riak_core_util:chash_key({?BUCKET, Key}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?SIMPLE_SERVICE),
    [{IndexNode, _Type}] = PrefList,
    IndexNode.
