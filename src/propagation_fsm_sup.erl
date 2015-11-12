-module(propagation_fsm_sup).

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
      [{propagation_fsm,
        {propagation_fsm, start_link, []},
        transient, 5000, worker, [propagation_fsm]}]
     }}.
