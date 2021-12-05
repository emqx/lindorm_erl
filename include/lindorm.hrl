-define(TIME_KEY, "time").

-define(PATH, "/api/v2/sql").
-define(QUERY_DB, "?database=").

-define(DEFAULT_BATCH_SIZE, 100).
-define(DEFAULT_BATCH_INTERVAL, 100).

-record(lindorm_client, {
    url                             :: binary()  | string(),
    database                        :: binary(),
    authorization_basic             :: undefined | binary(),
    pool                            :: term()
}).

-record(lindorm_ts_data,
    {
        table       :: binary(),
        time        :: integer() | undefined,
        tags        :: list() | map() | undefined,
        fields      :: list() |  map()
    }
).
