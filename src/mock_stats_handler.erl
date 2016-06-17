-module(mock_stats_handler).

-include("saturn_leaf.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([init/1,
         add_remote/3,
         add_update/3,
         compute_raw/3,
         merge_raw/2,
         compute_cdf/3,
         compute_cdf_from_orddict/1,
         clean/2]).

init(_Name) ->
    nothing.

clean(_Data, _Name) ->
    nothing.

add_remote(_Data, _Sender, _TimeStamp) ->
    nothing.

add_update(_Data, _Sender, _TimeStamp) ->
    nothing.

compute_raw(_Data, _From, _Type) ->
    nothing.

merge_raw(_FinalList, _NewList) ->
    nothing.

compute_cdf_from_orddict(_List) ->
    nothing.

compute_cdf(_Data, _From, _Type) ->
    nothing.
