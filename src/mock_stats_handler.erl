-module(mock_stats_handler).

-include("saturn_leaf.hrl").

-export([init/0,
         add_remote/2,
         add_update/2,
         compute_cdf/3,
         clean/1]).

init() ->
    nothing.

clean(_Data) ->
    nothing.

add_remote(_Data, _Label) ->
    nothing.

add_update(_Data, _Label) ->
    nothing.

compute_cdf(_Data, _From, _Type) ->
    nothing.
