-module(kofta_util).

-export([error_to_atom/1]).

-include("kofta.hrl").


-spec error_to_atom(ErrorID) -> ErrorName when
    ErrorID :: integer(),
    ErrorName :: atom().

error_to_atom(0) ->
    ok;
error_to_atom(ErrorID) ->
    Message = case ErrorID of
        -1 -> unexpected_server_error;
        1 -> offset_out_of_range;
        2 -> invalid_message;
        3 -> unknown_topic_or_partition;
        4 -> invalid_message_size;
        5 -> leader_not_available;
        6 -> not_leader_for_partition;
        7 -> request_timed_out;
        8 -> broker_not_available;
        9 -> replica_not_available;
        10 -> message_size_to_large;
        11 -> stale_controller_epoch_code;
        12 -> offset_metadata_too_large;
        14 -> offsets_load_in_progress;
        15 -> consumer_coordinator_not_available;
        16 -> not_coordinator_for_consumer;
        _ ->
            lager:warning("Got unknown ErrorID: ~p", [ErrorID]),
            unknown
    end,
    ?INCREMENT_COUNTER([kofta, errors, Message]),
    Message.
