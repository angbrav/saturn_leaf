-module(saturn_leaf_converger).
-behaviour(gen_server).

-include("saturn_leaf.hrl").
-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).
-export([handle/1,
         notify_update/1]).

-record(state, {labels_queue :: queue(),
                ops_dict :: dict()}).
                

start_link() ->
    gen_server:start({global, ?MODULE}, ?MODULE, [], []).

handle(Message) ->
    gen_server:call(?MODULE, Message, infinity).

notify_update(Label) ->
    gen_server:cast(?MODULE, {update_completed, Label}).

init([]) ->
    {ok, #state{labels_queue=queue:new(),
                ops_dict=dict:new()}}.

handle_call({new_stream, Stream}, _From, S0=#state{labels_queue=Labels0, ops_dict=_Ops0}) ->
    case length(Labels0) of
        0 ->
            [Label|_Tail] = Stream,
            check_match(Label, S0)
    end,
    Labels1 = queue:join(Labels0, Stream),
    {reply, ok, S0#state{labels_queue=Labels1}}.

handle_cast({update_completed, Label}, S0=#state{labels_queue=Labels0, ops_dict=Ops0}) ->
    case queue:peek(Labels0) of
        {value, Label} ->
            Labels1 = queue:drop(Labels0),
            Ops1 = dict:erase(Label, Ops0),
            S1 = S0#state{labels_queue=Labels1, ops_dict=Ops1},
            case queue:peek(Labels1) of
                {value, NextLabel} ->
                    check_match(NextLabel, S1)
            end,
            {noreply, S1};
        {value, _} ->
            lager:error("Head does not much with newly processed update"),
            {noreply, S0};
        _ ->
            lager:error("Empty queue"),
            {noreply, S0}
    end;

handle_cast({new_operation, Label, Key, Value}, S0=#state{labels_queue=Labels0, ops_dict=Ops0}) ->
    Ops1 = dict:store(Label, {Key, Value}, Ops0),
    case queue:peek(Labels0) of
        {value, Label} ->
            {Clock, _} = Label,
            ok = saturn_leaf_producer:new_clock(Clock),
            ?BACKEND_CONNECTOR_FSM:start_fsm([propagation, {Key, Value, Label}])
    end,
    {noreply, S0#state{ops_dict=Ops1}};

handle_cast(_Info, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

check_match(Label, _S0=#state{ops_dict=Ops0}) ->
    case dict:find(Label, Ops0) of
        {ok, Operation} ->
            {Clock, _} = Label,
            ok = saturn_leaf_producer:new_clock(Clock),
            riak_update_fsm_sup:start_child([propagation, Label, Operation])
    end.
