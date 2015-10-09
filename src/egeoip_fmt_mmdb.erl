%% @author Bill Morgan
%% @copyright (C) 2015, AdRoll
%% @doc This module reads the .mmdb MaxMind DB 2.0 binary format,
%%      as specified here: https://maxmind.github.io/MaxMind-DB/

-module(egeoip_fmt_mmdb).
-author("billmorgan").

-include("egeoip.hrl").

%% API
-export([read_structures/2]).

read_structures(Path, Data) ->
    Max = ?MMDB_METADATA_MAX_SIZE,
    SeekStart = size(Data) - size(?MMDB_METADATA_SEQUENCE),
    read_structures(Path, Data, SeekStart, Max).

read_structures(Path, Data, _Seek, 0) ->
    #geoipdb{segments = ?GEOIP_COUNTRY_BEGIN,
        data = Data,
        filename = Path};
read_structures(Path, Data, Seek, N) when N > 0 ->
    MetaDataSize = size(?MMDB_METADATA_SEQUENCE),
    <<_:Seek/binary, Delim:MetaDataSize/binary, _/binary>> = Data,
    case Delim of
        ?MMDB_METADATA_SEQUENCE ->
            {Metadata, _} = read_field(Data, Seek+MetaDataSize),
            Type = mmdb,
            Segments = Metadata,
            Length = 0,
            #geoipdb{type = Type,
                segments = Segments,
                record_length = Length,
                data = Data,
                filename = Path};
        _ ->
            read_structures(Path, Data, Seek - 1, N - 1)
    end;
read_structures(_,_,_,_) ->
    throw(parse_error_no_metadata_sequence).

read_field(Data, Seek) ->
    <<_:Seek/binary, Type:3/integer, SizePart:5/integer, _/binary>> = Data,
    NewSeek = Seek + 1,
    case Type of
        ?MMDB_TYPE_POINTER -> {unimplemented, Seek};
        ?MMDB_TYPE_UT8 -> read_utf8(Data, NewSeek, SizePart);
        ?MMDB_TYPE_DOUBLE -> {unimplemented, Seek};
        ?MMDB_TYPE_BYTES -> {unimplemented, Seek};
        ?MMDB_TYPE_UINT16 -> read_uint16(Data, NewSeek, SizePart);
        ?MMDB_TYPE_UINT32 -> read_uint32(Data, NewSeek, SizePart);
        ?MMDB_TYPE_MAP -> read_map(Data, NewSeek, SizePart);
        ?MMDB_TYPE_EXTENDED -> read_extended_field(Data, NewSeek, SizePart)
    end.

read_extended_field(Data, Seek, SizePart) ->
    <<_:Seek/binary, ExtendedType:8/integer, _/binary>> = Data,
    NewSeek = Seek + 1,
    Type = ExtendedType + ?MMDB_TYPE_MAP,
    case Type of
        ?MMDB_TYPE_INT32 -> {unimplemented, Seek};
        ?MMDB_TYPE_UINT64 -> read_uint64(Data, NewSeek, SizePart);
        ?MMDB_TYPE_UINT128 -> read_uint128(Data, NewSeek, SizePart);
        ?MMDB_TYPE_ARRAY -> read_array(Data, NewSeek, SizePart);
        ?MMDB_TYPE_DATA_CACHE_CONTAINER -> {unimplemented, Seek};
        ?MMDB_TYPE_END_MARKER -> {unimplemented, Seek};
        ?MMDB_TYPE_BOOLEAN -> {unimplemented, Seek};
        ?MMDB_TYPE_FLOAT -> {unimplemented, Seek}
    end.

read_payload_size(Data, Seek, SizePart) ->
    case SizePart of
        29 ->
            <<_:Seek/binary, Remaining:8/integer, _/binary>> = Data,
            {29 + Remaining, Seek + 1};
        30 ->
            <<_:Seek/binary, Remaining:16/integer, _/binary>> = Data,
            {30 + Remaining, Seek + 2};
        31 ->
            <<_:Seek/binary, Remaining:24/integer, _/binary>> = Data,
            {31 + Remaining, Seek + 3};
        Size -> {Size, Seek}
    end.

read_utf8(Data, Seek, SizePart) ->
    {Size, NewSeek} = read_payload_size(Data, Seek, SizePart),
    <<_:NewSeek/binary, Text:Size/binary, _/binary>> = Data,
    {Text, NewSeek + Size}.

read_uint16(Data, Seek, Size) ->
    read_uint(Data, Seek, Size, 2).

read_uint32(Data, Seek, Size) ->
    read_uint(Data, Seek, Size, 4).

read_uint64(Data, Seek, Size) ->
    read_uint(Data, Seek, Size, 8).

read_uint128(Data, Seek, Size) ->
    read_uint(Data, Seek, Size, 16).

read_uint(_Data, Seek, 0, _Limit) ->
    {0, Seek};
read_uint(_Data, _Seek, Size, Limit) when Size > Limit ->
    throw(parse_error_uint_size_too_big);
read_uint(Data, Seek, Size, _Limit) ->
    SizeInBits = Size * 8,
    <<_:Seek/binary, Int:SizeInBits/integer, _/binary>> = Data,
    {Int, Seek + Size}.

read_map(Data, Seek, SizePart) ->
    {NumPairs, NewSeek} = read_payload_size(Data, Seek, SizePart),
    read_map(Data, NewSeek, NumPairs, #{}).

read_map(_Data, Seek, 0, Acc) ->
    {Acc, Seek};
read_map(Data, Seek, NumPairs, Acc) ->
    {Key, ValSeek} = read_field(Data, Seek),
    {Val, NewSeek} = read_field(Data, ValSeek),
    read_map(Data, NewSeek, NumPairs - 1, Acc#{Key => Val}).

read_array(_Data, Seek, _SizePart) ->
    {unimplemented, Seek}.
