-module(stats_cdf_cure_handler_mock).

-include("saturn_leaf.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([init/1,
         add_remote/3,
         add_update/3,
         add_gst/2,
         clean/2]).

init(_Name) ->
    mock.

clean(Data, _Name) ->
    Data.

add_update(Data, _Sender, _TimeStamp) ->
    Data.

add_remote(Data, _Sender, _TimeStamp) ->
    Data.

add_gst(Data, _GST) ->
    Data.

