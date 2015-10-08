%% @author Bill Morgan
%% @copyright (C) 2015, AdRoll
%% @doc This module reads the .dat MaxMind database binary format,
%%      also refered to as "GeoIP Legacy format (dat)",
%%      such as the included MaxMind GeoLite City database.

-module(egeoip_fmt_dat).
-author("billmorgan").

-include("egeoip.hrl").

%% API
-export([read_structures/4]).

read_structures(Path, Data, _Seek, 0) ->
    #geoipdb{segments = ?GEOIP_COUNTRY_BEGIN,
        data = Data,
        filename = Path};
read_structures(Path, Data, Seek, N) when N > 0 ->
    <<_:Seek/binary, Delim:3/binary, _/binary>> = Data,
    case Delim of
        <<255, 255, 255>> ->
            <<_:Seek/binary, _:3/binary, DbType, _/binary>> = Data,
            Type = case DbType >= 106 of
                       true ->
                           DbType - 105;
                       false ->
                           DbType
                   end,
            Segments = case Type of
                           ?GEOIP_REGION_EDITION_REV0 ->
                               ?GEOIP_STATE_BEGIN_REV0;
                           ?GEOIP_REGION_EDITION_REV1 ->
                               ?GEOIP_STATE_BEGIN_REV1;
                           ?GEOIP_COUNTRY_EDITION ->
                               ?GEOIP_COUNTRY_BEGIN;
                           ?GEOIP_PROXY_EDITION ->
                               ?GEOIP_COUNTRY_BEGIN;
                           ?GEOIP_NETSPEED_EDITION ->
                               ?GEOIP_COUNTRY_BEGIN;
                           _ ->
                               read_segments(Type, Data, Seek + 4)
                       end,
            Length = case Type of
                         ?GEOIP_ORG_EDITION ->
                             ?ORG_RECORD_LENGTH;
                         ?GEOIP_ISP_EDITION ->
                             ?ORG_RECORD_LENGTH;
                         _ ->
                             ?STANDARD_RECORD_LENGTH
                     end,
            #geoipdb{type = Type,
                segments = Segments,
                record_length = Length,
                data = Data,
                filename = Path};
        _ ->
            read_structures(Path, Data, Seek - 1, N - 1)
    end;
read_structures(_,_,_,_) -> throw(metadata_parse_failure).

read_segments(Type, Data, Seek) when Type == ?GEOIP_CITY_EDITION_REV0;
    Type == ?GEOIP_CITY_EDITION_REV1;
    Type == ?GEOIP_ORG_EDITION;
    Type == ?GEOIP_ISP_EDITION;
    Type == ?GEOIP_ASNUM_EDITION ->
    Bits = ?SEGMENT_RECORD_LENGTH * 8,
    <<_:Seek/binary, Segments:Bits/little, _/binary>> = Data,
    Segments.

