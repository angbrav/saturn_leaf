-module(saturn_leaf_producer).
-behaviour(gen_server).
-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).
-export([new_clock/1,
         generate_label/4,
         unblock_label/1]).

-record(state, {clock :: non_neg_integer(),
                upstream :: list(),
                myid}).
                

start_link() ->
    gen_server:start({global, ?MODULE}, ?MODULE, [], []).

new_clock(TS) ->
    gen_server:call(?MODULE, {new_clock, TS}, infinity).

generate_label(Proxy, UId, Key, Value) ->
    gen_server:call(?MODULE, {generate_label, Proxy, UId, Key, Value}, infinity).

unblock_label(Label) ->
    gen_server:cast(?MODULE, {unblock_label, Label}).

init([]) ->
    {ok, #state{clock=0}}.

handle_call({new_clock, TS}, _From, S0=#state{clock=Clock0}) ->
    Clock1 = max(TS, Clock0),
    {reply, ok, S0#state{clock=Clock1}}.

handle_cast({generate_label, Proxy, UId, Key, Value}, S0=#state{clock=Clock0, upstream=Upstream0}) ->
    Clock1 = Clock0 + 1,
    Upstream1 = Upstream0 ++ [{Clock1, blocked}],
    saturn_client_proxy:new_label(Proxy, UId, {Clock1, node()}, Key, Value),
    {noreply, S0#state{clock=Clock1, upstream=Upstream1}};
            
handle_cast({unblock_label, Label}, S0=#state{upstream=Upstream0}) ->
    {Clock, _} = Label,
    Index = saturn_utilities:binary_search(Upstream0, Label, fun(Item1, Item2) ->
                                                                    {Clock1, _} = Item1,
                                                                    {Clock2, _} = Item2,
                                                                    case Clock1>Clock2 of
                                                                        true ->
                                                                            greater;
                                                                        false ->
                                                                            case Clock1==Clock2 of
                                                                                true ->
                                                                                    equal;
                                                                                false ->
                                                                                    lesser
                                                                            end
                                                                    end
                                                                end), 
    Length = length(Upstream0),
    case Index of
        1 ->
            Upstream1 = lists:nthtail(1, Upstream0),
            {Stream0, Upstream2} = generate_labels(Upstream1, [Clock]), 
            case saturn_groups_manager:filter_stream_leaf(Stream0) of
                {ok, [], _} ->
                    noop;
                {ok, Stream1, {Host, Port}} ->
                    propagation_fsm_sup:start_fsm(Port, Host, {new_stream, Stream1})
            end;
        Length ->
            Upstream2 = lists:droplast(Upstream0) ++ {Clock, unblocked};
        _ ->
            Upstream2 = lists:sublist(Upstream0, Index-1) ++ [Clock, unblocked] ++ lists:nthtail(Index, Upstream0)
    end,
    {noreply, S0#state{upstream=Upstream2}};

handle_cast(_Info, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

generate_labels([], Acc) ->
    {Acc, []};

generate_labels([H|T], Acc) ->
    case H of
        {Clock, unblocked} ->
            generate_labels(T, Acc ++ [Clock]);
        _ ->
            {Acc, T}
    end.
