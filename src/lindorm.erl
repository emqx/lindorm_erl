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

-export([ start/3
        , stop/1]).

-export([ write/2]).

start(Name, PoolSize, LindormOptions) ->
    Opts = [
          {pool_size, 10},
          {pool_type, round_robin},
          {auto_reconnect, 3},
          {lindorm, LindormOptions}
        ],
    ecpool:start_pool(Name, lindorm_worker, Opts).

stop(Name) ->
    ecpool:stop_sup_pool(Name).

write(Name, Data) ->
    ecpool:with_client(Name, fun(Worker) -> lindorm_worker:write(Worker, Data) end).
