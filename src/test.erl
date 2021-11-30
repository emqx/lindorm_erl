-module(test).
-include("lindorm.hrl").

-export([start/0]).

start() ->
    application:ensure_all_started(lindorm_erl),
    Data = #lindorm_ts_data{
        table = <<"demo_sensor">>,
        fields = #{
            <<"temperature">> => 10.11,
            <<"humidity">> => 1110.111
        },
        tags = #{
            <<"device_id">> => <<"t_device_id1">>,
            <<"region">> => <<"t_region1">>
        }
    },
    Data2 = #lindorm_ts_data{
        table = <<"demo_sensor">>,
        fields = #{
            <<"temperature">> => 22.22,
            <<"humidity">> => 22222.222
        },
        tags = #{
            <<"device_id">> => <<"t_device_id2">>,
            <<"region">> => <<"t_region2">>
        }
    },
    Encode = lindorm_sql:trans([Data, Data2]),
    LindormOptions = #{
        <<"pool">> => demo_pool,
        <<"database">> => <<"DemoDB1">>,
        <<"batch_size">> => 5,
        <<"url">> => <<"http://ld-wz9r1960qay9vdv6r-proxy-tsdb-pub.lindorm.rds.aliyuncs.com:8242">>
    },
    StartPool = lindorm:start(demo_lindorm, 4, LindormOptions),
    Res = lindorm:write(demo_lindorm, [Data, Data2]),
    io:format("~n~p ~n", [Res]).


