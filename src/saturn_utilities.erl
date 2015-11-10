-module(saturn_utilities).
-export([binary_search/3]).

binary_search(List, N, Comparator) ->
    Length = length(List),
    Middle = (Length + 1) div 2, %% saves us hassle with odd/even indexes
    case Middle of
        0 ->
            empty_list;
        _ ->
            Item = lists:nth(Middle, List),
            case Item  of
                N ->
                    Middle; 
                _ ->
                    case Comparator(Item, N) of
                        greater ->
                            binary_search(lists:sublist(List, Length - Middle), N, Comparator); %% LT, search on left side
                        _ ->
                            binary_search(lists:nthtail(Middle, List), N, Comparator)           %% GT, search on right side
                    end
            end
    end.

