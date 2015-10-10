-module(egeoip_fmt_mmdb_tests).
-include_lib("eunit/include/eunit.hrl").
-include("egeoip.hrl").

parse_geoip2_city_test() ->
    Path = "../deps/mmdb_spec/test-data/GeoIP2-City-Test-Broken-Double-Format.mmdb",
    {ok, Data} = file:read_file(Path),
    Returned = egeoip_fmt_mmdb:read_structures(Data),
    ?assert(is_record(Returned, geoipdb)),
    ?assert(Returned#geoipdb.type =:= mmdb),
    io:format("~p~n", [Returned#geoipdb.segments]).

parse_test() ->
    BaseDir = "../deps/mmdb_spec/test-data/",
    Files = ["GeoIP2-City-Test.mmdb"],
    ok = lists:foldl(fun(File, ok) ->
                    {ok, Data} = file:read_file(BaseDir ++ File),
                    Returned = egeoip_fmt_mmdb:read_structures(Data),
                    ?assert(is_record(Returned, geoipdb)),
                    ?assert(Returned#geoipdb.type =:= mmdb),
                    ok end,
                     ok, Files),
    ok.

missing_metadata_separator_test() ->
    SizeBits = (?MMDB_METADATA_MAX_SIZE + 256) * 8,
    Data = <<0:SizeBits>>,
    Returned = egeoip_fmt_mmdb:read_structures(Data),
    ?assert(is_record(Returned, geoipdb)),
    ?assert(Returned#geoipdb.type =/= mmdb).
