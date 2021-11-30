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
-module(lindorm_sql).

-include("lindorm.hrl").

-export([trans/1]).

trans(List) when is_list(List) ->
    First = hd(List),
    {Header, TagKeys, FiledKeys} = sql_header_part(First),
    ValuesBinary = append_binary([to_value(Data, TagKeys, FiledKeys) || Data <- List]),
    <<Header/binary, ValuesBinary/binary>>;

trans(Data) when is_record(Data, lindorm_ts_data) ->
    {Header, TagKeys, FiledKeys} = sql_header_part(Data),
    Value = to_value(Data, TagKeys, FiledKeys),
    <<Header/binary, Value/binary>>.

sql_header_part(#lindorm_ts_data{table = Table, tags = Tags, fields = Fields}) ->
    TagsKeys = keys(Tags),
    TagsKeysBinary = append_binary(TagsKeys),

    FieldsKeys = keys(Fields),
    FieldsKeysBinary = append_binary(FieldsKeys),

    Time = time_key(TagsKeys, FieldsKeys),

    Header = <<
        "INSERT INTO ", Table/binary, " (",
            TagsKeysBinary/binary,
            Time/binary,
            FieldsKeysBinary/binary,
        ") VALUES "
    >>,
    {Header, TagsKeys, FieldsKeys}.

time_key(TagKeys, FiledKeys) ->
    case {length(TagKeys), length(FiledKeys)} of
        {0, 0} -> <<?TIME_KEY>>;
        {0, L} when L > 0 -> <<?TIME_KEY, ",">>;
        {L, 0} when L > 0 -> <<",", ?TIME_KEY>>;
        {L, L2} when L > 0 andalso L2 > 0  -> <<",", ?TIME_KEY, ",">>
    end.

time_key(Time, TagKeys, FiledKeys) ->
    case {length(TagKeys), length(FiledKeys)} of
        {0, 0} -> Time;
        {0, L} when L > 0 -> <<Time/binary, ",">>;
        {L, 0} when L > 0 -> <<",", Time/binary>>;
        {L, L2} when L > 0 andalso L2 > 0  -> <<",", Time/binary, ",">>
    end.

to_value(Data = #lindorm_ts_data{time = undefined}, TagKeys, FiledKeys) ->
    to_value(Data#lindorm_ts_data{time = os:system_time(millisecond)}, TagKeys, FiledKeys);
to_value(#lindorm_ts_data{tags = Tags, time = Time, fields = Fields}, TagKeys, FiledKeys) ->
    TagsValues = get_values(TagKeys, Tags),
    TimeValue = time_key(to_binary(Time), TagKeys, FiledKeys),
    FieldsValues = get_values(FiledKeys, Fields),
    <<
        "(",
        TagsValues/binary,
        TimeValue/binary,
        FieldsValues/binary,
        ")"
    >>.

get_values([], _Data) -> <<>>;
get_values([Key | Keys], Data) ->
    get_values(Keys, Data, to_binary(get_value(Key, Data))).

get_values([], _Data, Res) ->
    Res;
get_values([Key | Keys], Data, Res) ->
    Binary = to_binary(get_value(Key, Data)),
    get_values(Keys, Data, <<Res/binary, ",", Binary/binary>>).


keys(undefined) -> [];
keys(KVs) when is_map(KVs) -> maps:keys(KVs);
keys(KVs) when is_list(KVs) -> proplists:get_keys(KVs).

get_value(K, KVs) when is_map(KVs)-> maps:get(K, KVs);
get_value(K, KVs) when is_list(KVs)-> proplists:get_value(K, KVs).

append_binary(Values) when is_list(Values) ->
    lists:foldl(fun append_value/2, <<>>, Values).

append_value(Value, <<>>) -> Value;
append_value(Value, Data) -> <<Data/binary, ",", Value/binary>>.

to_binary(true)                 -> <<"true">>;
to_binary(false)                -> <<"false">>;
to_binary(D) when is_binary(D)  -> <<"\'", D/binary, "\'">>;
to_binary(D) when is_integer(D) -> erlang:integer_to_binary(D);
to_binary(D) when is_float(D)   -> erlang:float_to_binary(D, [compact, {decimals, 12}]).
