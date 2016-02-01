-module(saturn_test_utilities).

-export([eventual_read/4,
         eventual_read/3]).

eventual_read(Key, Node, ExpectedResult) ->
    eventual_read(Key, Node, ExpectedResult, 0).

eventual_read(Key, Node, ExpectedResult, Clock) ->
    Result=rpc:call(Node, saturn_leaf, read, [Key, Clock]),
    case Result of
        {ok, {ExpectedResult, _Clock}} -> Result;
        _ ->
            lager:info("I read: ~p, expecting: ~p",[Result, ExpectedResult]),
            timer:sleep(500),
            eventual_read(Key, Node, ExpectedResult)
    end.
