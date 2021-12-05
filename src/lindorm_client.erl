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
-module(lindorm_client).

-include("lindorm.hrl").

-export([ init/2
        , status/1
        , write/2
        , stop/1
        ]).

init(Name, Options = #{database := DB, url := Url, pool_size := PoolSize}) ->
    Client = #lindorm_client{
        url = <<Url/binary, ?PATH, ?QUERY_DB, DB/binary>>,
        database = maps:get(database, Options),
        pool = Name
    },
    PoolOptions = [
        {pool_size, PoolSize},
        {timeout, 150000},
        {max_connections, 100}
    ],
    case hackney_pool:start_pool(Name, PoolOptions) of
        ok ->
            {ok, maybe_auth(Client, Options)};
        Error ->
            Error
    end.

status(Client = #lindorm_client{database = DB}) ->
    case exec_sql(Client, <<"SHOW DATABASES">>) of
        {ok, Res} ->
            DataBases = maps:get(<<"rows">>, jsx:decode(Res,[return_maps]), []),
            case lists:member([DB], DataBases) of
                true ->
                    {ok, Res};
                false ->
                    {error, {database_not_found, DB, DataBases}}
            end;
        Error ->
            Error
    end.

write(Client, Data) ->
    exec_sql(Client, lindorm_sql:trans(Data)).

stop(#lindorm_client{pool = Pool}) ->
    hackney_pool:stop_pool(Pool).

%%--------------------------------------------------------------------
%% internal
maybe_auth(Client, Options) ->
    UserName = maps:get(username, Options, undefined),
    PassWord = maps:get(password, Options, undefined),
    case {UserName, PassWord} of
        {_, undefined} -> Client;
        {undefined, _} -> Client;
        {_, _} ->
            Base64 = base64:encode(<<UserName/binary, ":", PassWord/binary>>),
            Auth = <<"Basic ", Base64/binary>>,
            Client#lindorm_client{authorization_basic = Auth}
    end.

exec_sql(Client = #lindorm_client{pool = Pool, url = Url}, SQL) ->
    Headers = headers(Client),
    Options = [{pool, Pool},
        {connect_timeout, 10000},
        {recv_timeout, 30000},
        {follow_redirectm, true},
        {max_redirect, 5},
        with_body],
    case hackney:request(post, Url, Headers, SQL, Options) of
        {ok, StatusCode, _Headers, ResponseBody}
            when StatusCode =:= 200
            orelse StatusCode =:= 204 ->
            {ok, ResponseBody};
        {ok, StatusCode, _Headers, ResponseBody} ->
            {error, {StatusCode, ResponseBody}};
        {error, Reason} ->
            {error, Reason}
    end.

headers(#lindorm_client{authorization_basic = undefined}) ->
    [{<<"Content-Type">>, <<"text/plain">>}];
headers(#lindorm_client{authorization_basic = Auth}) ->
    [
        {<<"Content-Type">>, <<"text/plain">>},
        {<<"Authorization">>, Auth}
    ].
