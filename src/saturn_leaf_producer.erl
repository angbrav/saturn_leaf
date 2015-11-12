-module(saturn_leaf_producer).
-behaviour(gen_server).

-include("saturn_leaf.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).
-export([new_clock/1,
         generate_label/4,
         unblock_label/1]).

-record(state, {clock :: non_neg_integer(),
                upstream :: list()}).
                

start_link() ->
    gen_server:start({global, ?MODULE}, ?MODULE, [], []).

new_clock(TS) ->
    gen_server:call({global, ?MODULE}, {new_clock, TS}, infinity).

generate_label(Proxy, UId, Key, Value) ->
    gen_server:cast({global, ?MODULE}, {generate_label, Proxy, UId, Key, Value}).

unblock_label(Label) ->
    gen_server:cast({global, ?MODULE}, {unblock_label, Label}).

init([]) ->
    {ok, #state{clock=0, upstream=[]}}.

handle_call({new_clock, TS}, _From, S0=#state{clock=Clock0}) ->
    Clock1 = max(TS, Clock0),
    {reply, ok, S0#state{clock=Clock1}}.

handle_cast({generate_label, Proxy, UId, Key, Value}, S0=#state{clock=Clock0, upstream=Upstream0}) ->
    Clock1 = Clock0 + 1,
    Label = {Key, Clock1, node()},
    Upstream1 = Upstream0 ++ [{Label, blocked}],
    saturn_proxy_vnode:new_label(Proxy, UId, Label, Key, Value),
    {noreply, S0#state{clock=Clock1, upstream=Upstream1}};
            
handle_cast({unblock_label, Label}, S0=#state{upstream=Upstream0}) ->
    Index = saturn_utilities:binary_search(Upstream0, {Label, unblocked}, fun(Item1, Item2) ->
                                                                    {{_K1, Clock1, _N1}, _} = Item1,
                                                                    {{_K2, Clock2, _N2}, _} = Item2,
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
        {ok, 1} ->
            Upstream1 = lists:nthtail(1, Upstream0),
            {Stream0, Upstream2} = generate_labels(Upstream1, [Label]),
            lager:info("Generated stream: ~p",[Stream0]),
            case saturn_groups_manager:filter_stream_leaf(Stream0) of
                {ok, [], _} ->
                    noop;
                {ok, _, no_indexnode} ->
                    noop;
                {ok, Stream1, {Host, Port}} ->
                    propagation_fsm_sup:start_fsm(Port, Host, {new_stream, Stream1})
            end;
        {ok, Length} ->
            Upstream2 = lists:droplast(Upstream0) ++ {Label, unblocked};
        {ok, _} ->
            Upstream2 = lists:sublist(Upstream0, Index-1) ++ [Label, unblocked] ++ lists:nthtail(Index, Upstream0);
        {error, not_found} ->
            Upstream2 = Upstream0,
            lager:error("Binary search could not find the element")
    end,
    {noreply, S0#state{upstream=Upstream2}};

handle_cast(_Info, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    lager:info("Weird message"),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

generate_labels([], Acc) ->
    {Acc, []};

generate_labels([H|T], Acc) ->
    case H of
        {Label, unblocked} ->
            generate_labels(T, Acc ++ [Label]);
        _ ->
            {Acc, [H|T]}
    end.

-ifdef(TEST).

generate_labels_test() ->
    List1 = [{1, unblocked},{2, unblocked},{3, blocked},{4, unblocked}],
    ?assertEqual({[1,2], [{3, blocked},{4, unblocked}]}, generate_labels(List1, [])),
    List2 = [{1, blocked},{2, unblocked},{3, blocked},{4, unblocked}],
    ?assertEqual({[], List2}, generate_labels(List2, [])),
    List3 = [{1, unblocked},{2, unblocked},{3, unblocked},{4, unblocked}],
    ?assertEqual({[1,2,3,4], []}, generate_labels(List3, [])).

-endif.
