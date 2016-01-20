-module(saturn_leaf_converger).
-behaviour(gen_server).

-include("saturn_leaf.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).
-export([handle/2]).

-record(state, {labels_queue :: queue(),
                ops_dict :: dict(),
                myid}).
               
reg_name(MyId) ->  list_to_atom(integer_to_list(MyId) ++ atom_to_list(?MODULE)). 

start_link(MyId) ->
    gen_server:start({global, reg_name(MyId)}, ?MODULE, [MyId], []).

handle(MyId, Message) ->
    lager:info("Message received: ~p", [Message]),
    gen_server:call({global, reg_name(MyId)}, Message, infinity).

init([MyId]) ->
    {ok, #state{labels_queue=queue:new(),
                ops_dict=dict:new(),
                myid=MyId}}.

handle_call({new_stream, Stream, _SenderId}, _From, S0=#state{labels_queue=Labels0, ops_dict=_Ops0}) ->
    lager:info("New stream received. Label: ~p", Stream),
    case queue:len(Labels0) of
        0 ->
            S1 = flush_queue(Stream, S0);
        _ ->
            Labels1 = queue:join(Labels0, queue:from_list(Stream)),
            S1 = S0#state{labels_queue=Labels1}
    end,
    {reply, ok, S1};

handle_call({new_operation, Label, Key, Value}, _From, S0=#state{labels_queue=Labels0, ops_dict=Ops0}) ->
    lager:info("New operation received. Label: ~p", [Label]),
    case queue:peek(Labels0) of
        {value, Label} ->
            {Key, _Clock, _} = Label,
            ok = execute_operation(Label, Value),
            Labels1 = queue:drop(Labels0),
            S1 = flush_queue(queue:to_list(Labels1), S0);
        _ ->
            Ops1 = dict:store(Label, {Key, Value}, Ops0),
            S1 = S0#state{ops_dict=Ops1}
    end,
    {reply, ok, S1}.

handle_cast(_Info, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

execute_operation({Key, Clock, _Node}, Value) ->
    DocIdx = riak_core_util:chash_key({?BUCKET, Key}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, ?SIMPLE_SERVICE),
    [{IndexNode, _Type}] = PrefList,
    saturn_proxy_vnode:propagate(IndexNode, Key, Value, Clock).

flush_queue([], S0) ->
    S0#state{labels_queue=queue:new()};

flush_queue([Label|Rest]=Labels, S0=#state{ops_dict=Ops0}) ->
    case dict:find(Label, Ops0) of
        {ok, {Key, Value}} ->
            {Key, _Clock, _} = Label,
            ok = execute_operation(Label, Value),
            Ops1 = dict:erase(Label, Ops0),
            flush_queue(Rest, S0#state{ops_dict=Ops1});
        _ ->
            S0#state{labels_queue=queue:from_list(Labels)}
    end.

-ifdef(TEST).

-endif.
