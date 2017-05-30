-module(saturn_internal_serv).
-behaviour(gen_server).

-include("saturn_leaf.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/1]).

-export([restart/1,
         set_tree/3,
         set_groups/2,
         handle/2]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {queues :: dict(),
                busy :: dict(),
                delays, %has to be in microsecs
                manager,
                myid}).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

reg_name(MyId) ->  list_to_atom(integer_to_list(MyId) ++ atom_to_list(?MODULE)).

start_link(MyId) ->
    gen_server:start_link({global, reg_name(MyId)}, ?MODULE, [MyId], []).

handle(MyId, Message) ->
    gen_server:cast({global, reg_name(MyId)}, Message).

restart(MyId) ->
    gen_server:call({global, reg_name(MyId)}, restart).

set_tree(MyId, TreeDict, NLeaves) ->
    gen_server:call({global, reg_name(MyId)}, {set_tree, TreeDict, NLeaves}, infinity).

set_groups(MyId, Groups) ->
    gen_server:call({global, reg_name(MyId)}, {set_groups, Groups}, infinity).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([MyId]) ->
    lager:info("Internal started: ~p", [MyId]),
    Manager = groups_manager:init_state(integer_to_list(MyId) ++ "internal"),
    Paths = Manager#state_manager.paths,
    lager:info("Paths: ~p", [dict:to_list(Paths)]),
    {ok, Nodes} = groups_manager:get_mypath(MyId, Paths),
    {Queues, Busy} = lists:foldl(fun(Node,{Queues0, Busy0}) ->
                                    Name = list_to_atom(integer_to_list(MyId) ++ "internal" ++ integer_to_list(Node)),
                                    {dict:store(Node, ets_queue:new(Name), Queues0), dict:store(Node, false, Busy0)}
                                 end, {dict:new(), dict:new()}, Nodes),
    Tree = Manager#state_manager.tree,
    {ok, Delays0} = groups_manager:get_delays_internal(Tree, MyId),
    Delays1 = lists:foldl(fun({Node, Delay}, Dict) ->
                            dict:store(Node, Delay*1000, Dict)
                          end, dict:new(), dict:to_list(Delays0)),
    {ok, #state{queues=Queues, myid=MyId, busy=Busy, delays=Delays1, manager=Manager}}.

handle_cast({new_stream, TaggedStream, IdSender}, S0=#state{queues=Queues0, busy=Busy0, delays=Delays, myid=MyId, manager=Manager}) ->
    lager:info("Received stream from ~p: ~p [Tagged]", [IdSender, TaggedStream]),
    Stream = lists:map(
        fun({Label,_}) -> Label end,
        TaggedStream),
    lager:info("Received stream from ~p: ~p ", [IdSender, Stream]),
    Paths = Manager#state_manager.paths,
    Groups = Manager#state_manager.groups,
    NLeaves = Manager#state_manager.nleaves,
    Queues1 = lists:foldl(fun(Label, Acc0) ->
                            BKey = Label#label.bkey,
                            {Bucket, _} = BKey,
                            lists:foldl(fun(Node, Acc1) ->
                                            case Node of
                                                IdSender ->
                                                    Acc1;
                                                _ ->
                                                    case Label#label.operation of
                                                        update ->
                                                            case groups_manager:interested(Node, Bucket, MyId, Groups, NLeaves, Paths) of
                                                                true ->
                                                                    Delay = dict:fetch(Node, Delays),
                                                                    Now = saturn_utilities:now_microsec(),
                                                                    Time = Now + Delay,
                                                                    Queue0 = dict:fetch(Node, Acc1),
                                                                    %lager:info("Inserting into queue: ~p", [Node]),
                                                                    Queue1 = ets_queue:in({Time, Label}, Queue0),
                                                                    %lager:info("New queue: ~p", [Queue1]),
                                                                    dict:store(Node, Queue1, Acc1);
                                                                false -> Acc1
                                                            end;
                                                        _ ->
                                                            Payload = Label#label.payload,
                                                            Destination = case Label#label.operation of
                                                                            remote_read -> Payload#payload_remote.to;
                                                                            remote_reply -> Payload#payload_reply.to
                                                                          end,
                                                            case groups_manager:on_path(Node, Destination, MyId, Paths, NLeaves) of
                                                                true ->
                                                                    Time = 0,
                                                                    Queue0 = dict:fetch(Node, Acc1),
                                                                    Queue1 = ets_queue:in({Time, Label}, Queue0),
                                                                    dict:store(Node, Queue1, Acc1);
                                                                false -> Acc1
                                                            end
                                                    end
                                            end
                                        end, Acc0, dict:fetch_keys(Queues0))
                          end, Queues0, Stream),
    {Queues2, Busy1} = lists:foldl(fun(Node, {Acc1, Acc2}) ->
                                    case dict:fetch(Node, Busy0) of
                                        false ->
                                            {NewQueue, NewPending} = deliver_labels(dict:fetch(Node, Queues1), Node, MyId, [], NLeaves),
                                            {dict:store(Node, NewQueue, Acc1), dict:store(Node, NewPending, Acc2)};
                                        true ->
                                            OldQueue = dict:fetch(Node, Queues1),
                                            {dict:store(Node, OldQueue, Acc1), dict:store(Node, true, Acc2)}
                                    end
                                   end, {dict:new(), dict:new()}, dict:fetch_keys(Queues1)),
    {noreply, S0#state{queues=Queues2, busy=Busy1}};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_call(restart, _From, S0=#state{busy=Busy0, queues=Queues0}) ->
    Queues1 = lists:foldl(fun({Node, Queue}, Acc) ->
                            dict:store(Node, ets_queue:clean(Queue), Acc)
                          end, dict:new(), dict:to_list(Queues0)),
    Busy1 = lists:foldl(fun({Node, _}, Acc) ->
                            dict:store(Node, false, Acc)
                        end, dict:new(), dict:to_list(Busy0)),
    {reply, ok, S0#state{queues=Queues1, busy=Busy1}};

handle_call({set_tree, Tree, Leaves}, _From, S0=#state{manager=Manager0, myid=MyId, queues=Queues}) ->
    Paths = groups_manager:path_from_tree_dict(Tree, Leaves),
    lager:info("Paths: ~p", [dict:to_list(Paths)]),
    {ok, Nodes} = groups_manager:get_mypath(MyId, Paths),
    lists:foreach(fun({_Node, Queue}) ->
                    ets_queue:delete(Queue)
                  end, dict:to_list(Queues)), 
    {Queues1, Busy} = lists:foldl(fun(Node,{Queues0, Busy0}) ->
                                    Name = list_to_atom(integer_to_list(MyId) ++ "internal" ++ integer_to_list(Node)),
                                    {dict:store(Node, ets_queue:new(Name), Queues0), dict:store(Node, false, Busy0)}
                                 end, {dict:new(), dict:new()}, Nodes),
    Manager1 = Manager0#state_manager{paths=Paths, tree=Tree, nleaves=Leaves},
    {reply, ok, S0#state{manager=Manager1, queues=Queues1, busy=Busy}};

handle_call({set_groups, RGroups}, _From, S0=#state{manager=Manager}) ->
    Table = Manager#state_manager.groups,
    ok = groups_manager:set_groups(Table, RGroups),
    {reply, ok, S0};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_info({deliver_labels, Node}, S0=#state{queues=Queues0, busy=Busy0, myid=MyId, manager=Manager}) ->
    NLeaves = Manager#state_manager.nleaves,
    {NewQueue, NewPending} = deliver_labels(dict:fetch(Node, Queues0), Node, MyId, [], NLeaves),
    Queues1 = dict:store(Node, NewQueue, Queues0),
    Busy1 = dict:store(Node, NewPending, Busy0),
    {noreply, S0#state{queues=Queues1, busy=Busy1}};

handle_info(Info, State) ->
    lager:info("Weird message: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
deliver_labels(Queue0, Node, MyId, Deliverables0, NLeaves) ->
    Now = saturn_utilities:now_microsec(),
    case ets_queue:peek(Queue0) of
        empty ->
            propagate_stream(Node, lists:reverse(Deliverables0), MyId, NLeaves),
            {Queue0, false};
        {value, {Time, Label}} when Time =< Now ->
            {_, Queue1} = ets_queue:out(Queue0),
            deliver_labels(Queue1, Node, MyId, [Label|Deliverables0], NLeaves);
        {value, {Time, _Label}} ->
            propagate_stream(Node, lists:reverse(Deliverables0), MyId, NLeaves),
            NextDelivery = trunc((Time - Now)/1000),
            erlang:send_after(NextDelivery, self(), {deliver_labels, Node}),
            {Queue0, true}
    end.
               
propagate_stream(_Node, [], _MyId, _NLeaves) ->
    done;

propagate_stream(Node, Stream, MyId, NLeaves) ->
    lager:info("Stream to propagate to ~p: ~p", [Node, Stream]),

  TaggedStream = case is_list(Stream) of
                   true -> [{
                     Label,
                     tag
                     %<<32,16,10,9,108,111,99,97,108,104,111,115,116,16,170,31,32,16,10,9,108,111,99,97,108,104,111,115,116,16,170,31,32,16,10,9,108,111,99,97,108,104,111,115,116,16,170,31,32,16,10,9,108,111,99,97,108,104,111,115,116,16,170,31, 32,16,10,9,108,111,99,97,108,104,111,115,116,16,170,31,32,16,10,9,108,111,99,97,108,104,111,115,116,16,170,31, 32,16,10,9,108,111,99,97,108,104,111,115,116,16,170,31,32,16,10,9,108,111,99,97,108,104,111,115,116,16,170,31>>
                   } || Label <- Stream];
                   false -> lol
                 end,
  lager:info("Stream to propagate to ~p: ~p [Tagged]", [Node, TaggedStream]),

  case groups_manager:is_leaf(Node, NLeaves) of
        true ->
            saturn_leaf_converger:handle(Node, {new_stream, TaggedStream, MyId});
        false ->
            saturn_internal_serv:handle(Node, {new_stream, TaggedStream, MyId})
    end.
