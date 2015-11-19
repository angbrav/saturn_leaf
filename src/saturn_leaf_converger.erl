-module(saturn_leaf_converger).
-behaviour(gen_server).

-include("saturn_leaf.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).
-export([handle/2,
         notify_update/2]).

-record(state, {labels_queue :: queue(),
                ops_dict :: dict(),
                myid}).
               
reg_name(MyId) ->  list_to_atom(integer_to_list(MyId) ++ atom_to_list(?MODULE)). 

start_link(MyId) ->
    gen_server:start({global, reg_name(MyId)}, ?MODULE, [MyId], []).

handle(MyId, Message) ->
    lager:info("Message received: ~p", [Message]),
    gen_server:call({global, reg_name(MyId)}, Message, infinity).

notify_update(MyId, Label) ->
    gen_server:cast({global, reg_name(MyId)}, {update_completed, Label}).

init([MyId]) ->
    {ok, #state{labels_queue=queue:new(),
                ops_dict=dict:new(),
                myid=MyId}}.

handle_call({new_stream, Stream, _SenderId}, _From, S0=#state{labels_queue=Labels0, ops_dict=_Ops0}) ->
    lager:info("New stream received. Label: ~p", Stream),
    case queue:len(Labels0) of
        0 ->
            [Label|_Tail] = Stream,
            check_match(Label, S0);
        _ ->
            noop
    end,
    Labels1 = queue:join(Labels0, queue:from_list(Stream)),
    {reply, ok, S0#state{labels_queue=Labels1}};

handle_call({new_operation, Label, Key, Value}, _From, S0=#state{labels_queue=Labels0, ops_dict=Ops0, myid=MyId}) ->
    lager:info("New operation received. Label: ~p", [Label]),
    Ops1 = dict:store(Label, {Key, Value}, Ops0),
    case queue:peek(Labels0) of
        {value, Label} ->
            {Key, Clock, _} = Label,
            ok = saturn_leaf_producer:new_clock(MyId, Clock),
            ?BACKEND_CONNECTOR_FSM:start_link(propagation, {Key, Value, Label, MyId});
        _ ->
            noop
    end,
    {reply, ok, S0#state{ops_dict=Ops1}}.

handle_cast({update_completed, Label}, S0=#state{labels_queue=Labels0, ops_dict=Ops0}) ->
    case queue:peek(Labels0) of
        {value, Label} ->
            Labels1 = queue:drop(Labels0),
            Ops1 = dict:erase(Label, Ops0),
            S1 = S0#state{labels_queue=Labels1, ops_dict=Ops1},
            case queue:peek(Labels1) of
                {value, NextLabel} ->
                    check_match(NextLabel, S1);
                _ ->
                    noop
            end,
            {noreply, S1};
        {value, _} ->
            lager:error("Head does not much with newly processed update"),
            {noreply, S0};
        _ ->
            lager:error("Empty queue"),
            {noreply, S0}
    end;

handle_cast(_Info, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

check_match(Label, _S0=#state{ops_dict=Ops0, myid=MyId}) ->
    case dict:find(Label, Ops0) of
        {ok, {Key, Value}} ->
            {Key, Clock, _} = Label,
            ok = saturn_leaf_producer:new_clock(MyId, Clock),
            ?BACKEND_CONNECTOR_FSM:start_link(propagation, {Key, Value, Label, MyId});
        _ ->
            noop
    end.
