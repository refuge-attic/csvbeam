
-module(csvbeam).

-export([process_csv_file/2]).

process_csv_file(File, DbName) ->
    {ok, IoDevice} = file:open(File, [read]),

    Server = couchbeam:server_connection(),
    {ok, Db} = couchbeam:open_or_create_db(Server, DbName),

    InitState = {Db, [], [], 0},
    ImportFunction = fun process_row/2,

    {ok, {_, _, RemainingDocs, N}} = ecsv:process_csv_file_with(IoDevice, ImportFunction, InitState),
    case length(RemainingDocs) of
        0 ->
            ok;
        _ ->
            io:format("Saving remaining docs: ~p~n", [length(RemainingDocs)]),
            couchbeam:save_docs(Db, RemainingDocs)
    end,

    io:format("~p rows have been imported in the Db ~p!~n", [N, DbName]),

    ok.

label_if_1k(N) ->
    Modulo = N rem 1000,
    case Modulo of
        0 ->
            io:format("--> ~p parsed rows~n", [N]);
        _ ->
            ok  
    end.

process_row(NewLine, {Db, Labels, Docs, N}) ->
    case N of
        0 ->
            %
            L = convert_row_to_label(NewLine),
            {Db, L, Docs, N+1};
        _ ->
            NbDocs = length(Docs),
            CurrentDocs = case NbDocs of
                500 ->
                    % time to save those
                    couchbeam:save_docs(Db, Docs),
                    io:format("Sending 500 more docs~n"),
                    [];
                _ ->
                    Docs
            end,
            Doc = convert_row_to_doc(Labels, NewLine),
            label_if_1k(N),
            {Db, Labels, [Doc | CurrentDocs], N+1}
    end.

convert_row_to_label(Row) ->
    lists:flatmap( fun(X) -> [iolist_to_binary([X])] end, Row ).

convert_row_to_doc(Labels, Row) ->
    Properties = lists:zipwith(
        fun(Label, Value) ->
            { Label, iolist_to_binary(Value) }
        end,
        Labels,
        Row
    ),

    { Properties }.
