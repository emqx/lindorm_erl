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

-export([ start/2
        , status/1
        , write/2
        , stop/1
        ]).

-export([ ts_write_message/4]).

start(Name, Options) ->
    lindorm_client:init(Name, Options).

status(Client) ->
    lindorm_client:status(Client).

write(Client, Data) ->
    lindorm_client:write(Client, Data).

stop(Client) ->
    lindorm_client:stop(Client).

ts_write_message(Table, Tags, Time, Fields) ->
    #lindorm_ts_data{
        table    = Table,
        tags     = Tags,
        time     = Time,
        fields   = Fields
    }.
