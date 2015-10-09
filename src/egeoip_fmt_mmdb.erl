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
        <<_:Seek/binary, Delim:14/binary, _/binary>> = Data,
    case Delim of
        ?MMDB_METADATA_SEQUENCE ->
            %<<_:Seek/binary, _:3/binary, DbType, _/binary>> = Data,
            Type = mmdb,
            Segments = undefined,
            Length = 0,
            #geoipdb{type = Type,
                segments = Segments,
                record_length = Length,
                data = Data,
                filename = Path};
        _ ->
            read_structures(Path, Data, Seek - 1, N - 1)
    end;
read_structures(_,_,_,_) -> {error, no_metadata_sequence}.


