-module(egeoip_fmt_mmdb_tests).
-include_lib("eunit/include/eunit.hrl").
-include("egeoip.hrl").

parse_geoip2_city_test() ->
    Path = "../deps/mmdb_spec/test-data/GeoIP2-City-Test.mmdb",
    {ok, Data} = file:read_file(Path),
    Returned = egeoip_fmt_mmdb:read_structures(Path, Data),
    ?assert(is_record(Returned, geoipdb)),
    ?assert(Returned#geoipdb.type =:= mmdb).


missing_metadata_separator_test() ->
    Path = "not/used.mmdb",
    SizeBits = (?MMDB_METADATA_MAX_SIZE + 256) * 8,
    Data = <<0:SizeBits>>,
    Returned = egeoip_fmt_mmdb:read_structures(Path, Data),
    ?assert(is_record(Returned, geoipdb)),
    ?assert(Returned#geoipdb.type =/= mmdb).
