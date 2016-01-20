-module(saturn_test_utilities).

-export([eventual_read/3]).

eventual_read(Key, Node, ExpectedResult) ->
    Result=rpc:call(Node, saturn_leaf, read, [Key]),
    case Result of
        {ok, {ExpectedResult, _Clock}} -> Result;
        _ ->
            lager:info("I read: ~p, expecting: ~p",[Result, ExpectedResult]),
            timer:sleep(500),
            eventual_read(Key, Node, ExpectedResult)
    end.
