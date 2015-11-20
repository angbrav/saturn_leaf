-module(saturn_leaf_propagation_fsm_sup).

-behavior(supervisor).

-export([start_fsm/1,
         start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_fsm(Args) ->
    supervisor:start_child(?MODULE, Args).

init([]) ->
    {ok, {{simple_one_for_one, 5, 10},
      [{saturn_leaf_propagation_fsm,
        {saturn_leaf_propagation_fsm, start_link, []},
        transient, 5000, worker, [saturn_leaf_propagation_fsm]}]
     }}.
