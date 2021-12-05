-module(test).
-include("lindorm.hrl").

-export([start/0]).


%%
%% 
%%
 
% SELECT

%   payload.temperature as temperature,
% payload.humidity as humidity,
% clientid,
% timestamp

% FROM

% "#"
%% 
%% 
%% 
start() ->
    application:ensure_all_started(lindorm_erl),
    Data = #lindorm_ts_data{
        table = <<"demo_sensor">>,
        fields = #{
            <<"temperature">> => 10.11,
            <<"humidity">> => 1110.111},
        tags = #{
            <<"device_id">> => <<"t_device_id1">>,
            <<"region">> => <<"t_region1">>}
    },
    Data2 = #lindorm_ts_data{
        table = <<"demo_sensor">>,
        fields = #{
            <<"temperature">> => 22.22,
            <<"humidity">> => 22222.222},
        tags = #{
            <<"device_id">> => <<"t_device_id2">>,
            <<"region">> => <<"t_region2">>}
    },
    Encode = lindorm_sql:trans([Data, Data2]),
    io:format(" ~nencode ~0p~n", [Encode]),
    LindormOptions = #{
        database => <<"DemoDB1">>,
        url => <<"http://ld-wz92i1mj8t4yd17a0-proxy-tsdb-pub.lindorm.rds.aliyuncs.com:8242">>,
        batch_result_handler => fun handler/2,
        username => <<"root">>,
        password => <<"root">>
    },
    Client = demo_pool,
    lindorm:start(Client, 4, LindormOptions),
    IsAlive = lindorm:status(Client),
    io:format("is alive ~p", [IsAlive]),
    Res = lindorm:write(Client, [Data, Data2]),
    io:format("~n~p ~n", [Res]),
    ok.

handler(A, B) ->
    io:format("~p ~p ~n", [A,B]).

