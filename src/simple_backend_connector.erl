-module(simple_backend_connector).

-include("saturn_leaf.hrl").

-export([update/2,
         read/2,
         propagation/2,
         connect/0
        ]).

update(_Connector, Payload)->
    {Key, Value, TimeStamp} = Payload,
    IndexNode = get_indexnode(Key),
    saturn_simple_backend_vnode:update(IndexNode, Key, {Value, TimeStamp}).

read(_Connector, Payload)->
    {Key} = Payload,
    IndexNode = get_indexnode(Key),
    saturn_simple_backend_vnode:read(IndexNode, Key).

propagation(_Connector, Payload)->
    {Key, Value, TimeStamp} = Payload,
    IndexNode = get_indexnode(Key),
    saturn_simple_backend_vnode:propagation(IndexNode, Key, {Value, TimeStamp}).

get_indexnode(Key) ->
    DocIdx = riak_core_util:chash_key({?BUCKET, Key}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?SIMPLE_SERVICE),
    [{IndexNode, _Type}] = PrefList,
    IndexNode.

connect() ->
    ok.
