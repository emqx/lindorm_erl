%%--------------------------------------------------------------------
%% Copyright (c) 2019-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------
-module(lindorm).
-include("lindorm.hrl").

-export([ start/3
        , write/2
        , sync_write/2
        , status/1
        , stop/1]).

-export([ ts_write_message/4]).

start(Client, PoolSize, LindormOptions) ->
    Opts = [
          {pool_size, PoolSize},
          {pool_type, round_robin},
          {auto_reconnect, 3},
          {lindorm, LindormOptions#{pool => Client}}
        ],
    ecpool:start_pool(Client, lindorm_worker, Opts).

status(Client) ->
    ecpool:with_client(Client, fun(Worker) -> lindorm_worker:status(Worker) end).

stop(Client) ->
    ecpool:stop_sup_pool(Client).

write(Client, Data) ->
    ecpool:with_client(Client, fun(Worker) -> lindorm_worker:write(Worker, Data) end).

sync_write(Client, Data) ->
    ecpool:with_client(Client, fun(Worker) -> lindorm_worker:sync_write(Worker, Data) end).

ts_write_message(Table, Tags, Time, Fields) ->
    #lindorm_ts_data{
        table    = Table,
        tags     = Tags,
        time     = Time,
        fields   = Fields
    }.
