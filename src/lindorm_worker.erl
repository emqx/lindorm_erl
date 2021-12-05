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
-module(lindorm_worker).
-behaviour(gen_server).

-include("lindorm.hrl").

-export([ write/2
        , sync_write/2
        , status/1
        ]).

-export([ start_link/1]).

-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-export([ connect/1]).

-define(SERVER, ?MODULE).

-record(state, {
    url                             :: binary()  | string(),
    database                        :: binary(),
    authorization_basic             :: undefined | binary(),
    batch                           :: undefined | list(),
    batch_size                      :: integer(),
    batch_interval                  :: integer(),
    pool                            :: term(),
    batch_result_handler            :: fun() | undefined %% fun(ApiResponse, Batch)
}).

status(Pid) ->
    gen_server:call(Pid, status).

write(Pid, Data) ->
    gen_server:call(Pid, {write, Data}).

sync_write(Pid, Data) ->
    gen_server:call(Pid, {sync_write, Data}).

connect(Opts) ->
    LindormOptions = proplists:get_value(lindorm, Opts),
    start_link(LindormOptions).

start_link(Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

init(Opts = #{url := Url0, pool := Pool, database := DB}) ->
    Url = <<Url0/binary, ?PATH, ?QUERY_DB, DB/binary>>,
    BatchResultHandler = maps:get(batch_result_handler, Opts, undefined),
    State = case maps:get(batch_size, Opts, undefined) of
        undefined ->
            #state{url = Url,
                   database = DB,
                   pool = Pool,
                   batch_size = ?DEFAULT_BATCH_SIZE,
                   batch = [],
                   batch_interval = ?DEFAULT_BATCH_INTERVAL,
                   batch_result_handler = BatchResultHandler};
        BatchSize ->
            #state{url = Url,
                   database = DB,
                   pool = Pool,
                   batch_size = BatchSize,
                   batch = [],
                   batch_interval = maps:get(batch_interval, Opts, ?DEFAULT_BATCH_INTERVAL),
                   batch_result_handler = BatchResultHandler}
    end,
    {ok, auth(Opts, State)}.

auth(Opts, State) ->
    UserName = maps:get(username, Opts, undefined),
    PassWord = maps:get(password, Opts, undefined),
    case {UserName, PassWord} of
        {_, undefined} -> State;
        {undefined, _} -> State;
        {_, _} ->
            Base64 = base64:encode(<<UserName/binary, ":", PassWord/binary>>),
            Auth = <<"Basic ", Base64/binary>>,
            State#state{authorization_basic = Auth}
    end.

handle_call({sync_write, Data}, _From, State) ->
    {reply, do_write(Data, State), State};

handle_call({write, Data}, _From, State = #state{batch = Batch0,
                                                 batch_size = BatchSize,
                                                 batch_interval = Interval}) ->
    Batch = append_batch(Data, Batch0),
    case {length(Batch0), length(Batch)} of
        {0, Len} when Len >= BatchSize ->
            do_write(lists:reverse(Batch), State),
            {reply, ok, State#state{batch = Batch}};
        {0, _} ->
            {reply, ok, State#state{batch = Batch}, Interval};
        {_, _} ->
            {reply, ok, State#state{batch = Batch}}
    end;

handle_call(status, _From, State) ->
    {reply, do_status(State), State}.

append_batch(Data, Batch) when is_record(Data, lindorm_ts_data) ->
    lists:append(Batch, [Data]);
append_batch(Data, Batch) when is_list(Data) ->
    lists:append(Batch, Data).

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(timeout, State = #state{batch = Batch, batch_result_handler = undefined}) ->
    do_write(lists:reverse(Batch), State),
    {noreply, State#state{batch = []}};
handle_info(timeout, State = #state{batch = Batch, batch_result_handler = Handler})
        when is_function(Handler, 2) ->
    Res = do_write(lists:reverse(Batch), State),
    Handler(Batch, Res),
    {noreply, State#state{batch = []}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

do_status(State = #state{database = DB}) ->
    case do_request(<<"SHOW DATABASES">>, State) of
        {ok, Res} ->
            DataBases = maps:get(<<"rows">>, jsx:decode(Res), []),
            case lists:member([DB], DataBases) of
                true ->
                    {ok, Res};
                false ->
                    {error, {database_not_found, DB, DataBases}}
            end;
        Error ->
            Error
    end.

do_write(Data, State) ->
    do_request(lindorm_sql:trans(Data), State).

do_request(SQL, State = #state{url = Url, pool = Pool}) ->
    Headers = headers(State),
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

headers(#state{authorization_basic = undefined}) ->
    [{<<"Content-Type">>, <<"text/plain">>}];
headers(#state{authorization_basic = Auth}) ->
    [
        {<<"Content-Type">>, <<"text/plain">>},
        {<<"Authorization">>, Auth}
    ].
