%% @author Bob Ippolito <bob@redivi.com>
%% @copyright 2006 Bob Ippolito

%% @doc Geolocation by IP address.

-module(egeoip2).
-author('bob@redivi.com').

-behaviour(gen_server).

%% record access API
-export([get/2]).
-export([record_fields/0]).

%% gen_server based API
-export([start/0, start/1, start_link/1, start_link/2, stop/0,
         lookup/1, reload/0, reload/1, filename/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, terminate/2, code_change/3,
         handle_info/2]).

%% in-process API
-export([new/1, new/0]).
-export([lookup/2]).


%% useful utility functions
-export([ip2long/1]).

-include("egeoip2.hrl").

%% geoip record API

%% @type geoip_atom() = country_code | country_code3 | country_name |
%%                      region | city | postal_code | latitude | longitude |
%%                      area_code | dma_code
%% @type geoip_field() = geoip_atom | [geoip_atom()]

%% @spec get(R::geoip(), Field::geoip_field()) -> term()
%% @doc Get a field from the geoip record returned by lookup.

get(R, country_code) ->
    R#geoip.country_code;
get(R, country_code3) ->
    R#geoip.country_code3;
get(R, country_name) ->
    R#geoip.country_name;
get(R, region) ->
    R#geoip.region;
get(R, city) ->
    R#geoip.city;
get(R, postal_code) ->
    R#geoip.postal_code;
get(R, latitude) ->
    R#geoip.latitude;
get(R, longitude) ->
    R#geoip.longitude;
get(R, area_code) ->
    R#geoip.area_code;
get(R, dma_code) ->
    R#geoip.dma_code;
get(R, List) when is_list(List) ->
    [get(R, X) || X <- List].

%% server API

%% @spec reload() -> ok
%% @doc Reload the existing database in this process and then change the
%%      state of the running server.
reload() ->
    reload(filename()).

%% @spec reload(Path) -> ok
%% @doc Load the database at Path in this process and then change the
%%      state of the running server with the new database.
reload(FileName) ->
    case new(FileName) of
        {ok, NewState} ->
            Workers = egeoip2_cluster:worker_names(),
            [gen_server:call(W, {reload, NewState})  || W <- tuple_to_list(Workers)];
        Error ->
            Error
    end.

%% @spec start() -> ok
%% @doc Start the egeoip2 application with the default database.
start() ->
    application:start(egeoip2).

%% @spec start(File) -> ok
%% @doc Start the egeoip2 application with the File as database.
start(File) ->
    application:load(egeoip2),
    application:set_env(egeoip2, dbfile, File),
    start().


%% @spec start_link(Name) -> {ok, Pid}
%% @doc Start the server using the default priv/GeoLitecity.dat.gz database.
%%      The process will be registered as Name
start_link(Name) ->
    start_link(Name, city).

%% @spec start_link(Name, Path) -> {ok, Pid}
%% @doc Start the server using the database at Path registered as Name.
start_link(Name, FileName) ->
    gen_server:start_link(
      {local, Name}, ?MODULE, FileName, []).

%% @spec stop() -> ok
%% @doc Stop the server.
stop() ->
    application:stop(egeoip2).

%% @spec lookup(Address) -> geoip()
%% @doc Get a geoip() record for the given address. Fields can be obtained
%%      from the record using get/2.
lookup(Address) when is_integer(Address) ->
    case whereis(egeoip2) of
        undefined ->
            Worker = get_worker(Address),
            gen_server:call(Worker, {lookup, Address});
        Pid ->
            unregister(egeoip2),
            register(egeoip2_0, Pid),
            FileName = gen_server:call(Pid, filename),
            [egeoip2_0 | Workers] = tuple_to_list(egeoip2_cluster:worker_names()),
            Specs = egeoip2_sup:worker(Workers, FileName),
            lists:map(fun(Spec) ->
                              {ok, _Pid} = supervisor:start_child(egeoip2_sup, Spec)
                      end, Specs),
            lookup(Address)
    end;
lookup(Address) ->
    case ip2long(Address) of
        {ok, Ip} ->
            lookup(Ip);
        Error ->
            Error
    end.

%% @spec record_fields() -> Fields::list()
%% @doc Get an ordered list of the geoip record fields
record_fields() ->
    record_info(fields, geoip).

%% @spec filename() -> string()
%% @doc Get the database filename currently being used by the server.
filename() ->
    gen_server:call(element(1, egeoip2_cluster:worker_names()), filename).

%% gen_server callbacks

%% @spec init(Path) -> {ok, State}
%% @doc initialize the server with the database at Path.
init(FileName) ->
    new(FileName).

%% @spec handle_call(Msg, From, State) -> term()
%% @doc gen_server callback.
handle_call(What,From,State) ->
    try
        do_handle_call(What,From,State)
    catch
        _:R ->
            log_error([{handle_call,What},{error,R}]),
            {reply,{error,R},State}
    end.

do_handle_call({lookup, Ip}, _From, State) when is_integer(Ip) ->
    {reply, lookup(State, Ip), State};
do_handle_call({lookup, Address}, _From, State) ->
    {ok, Ip} = ip2long(Address),
    {reply, lookup(State, Ip), State};
do_handle_call({reload, NewState}, _From, _State) ->
    {reply, ok, NewState};
do_handle_call(filename, _From, State) ->
    {reply, State#geoipdb.filename, State}.

%% @spec handle_cast(Msg, State) -> term()
%% @doc gen_server callback.
handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(What, State) ->
    log_error([{handle_cast, What}]),
    {noreply, State}.

%% @spec terminate(Reason, State) -> ok
%% @doc gen_server callback.
terminate(_Reason, _State) ->
    ok.

%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @doc gen_server callback.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @spec handle_info(Info, State) -> {noreply, State}
%% @doc gen_server callback.
handle_info(Info, State) ->
    log_error([{handle_info,Info}]),
    {noreply, State}.

%% Implementation
get_worker(Address) ->
    element(
        1 + erlang:phash2(Address, egeoip2_cluster:worker_count()),
        egeoip2_cluster:worker_names()
    ).

log_error(Info) ->
    error_logger:info_report([?MODULE|Info]).

%% @spec new() -> {ok, geoipdb()}
%% @doc Create a new geoipdb database record using the default
%%      priv/GeoLiteCity.dat.gz database.
new() ->
    new(city).

%% @spec new(Path) -> {ok, geoipdb()}
%% @doc Create a new geoipdb database record using the database at Path.
new(city) ->
    new(default_db(["GeoIPCity.dat", "GeoLiteCity.dat"]));
new(Path) ->
    case filelib:is_file(Path) of
        true ->
            {ok, Data} = file:read_file(Path),
            {ok, Meta} = geodata2_format:meta(Data),
            State = #geoipdb{meta=Meta,data=Data},
            {ok, State};
        false ->
	    {error, {geoip_db_not_found,Path}}
    end.

%% @spec lookup(D::geoipdb(), Addr) -> {ok, geoip()}
%% @doc Lookup a geoip record for Addr using the database D.
lookup(D, Addr) when is_integer(Addr) ->
    lookup_record(D, Addr).

default_db([]) ->
    not_found;
default_db([Path | Rest]) ->
    FullPath = priv_path([Path]),
    case lists:filter(fun filelib:is_file/1, [FullPath ++ ".gz", FullPath]) of
        [] ->
            default_db(Rest);
        [DbPath | _] ->
            DbPath
    end.

address_fast([N2, N1, N0, $. | Rest], Num, Shift) when Shift >= 8 ->
    case list_to_integer([N2, N1, N0]) of
        N when N =< 255 ->
            address_fast(Rest, Num bor (N bsl Shift), Shift - 8)
    end;
address_fast([N1, N0, $. | Rest], Num, Shift) when Shift >= 8 ->
    case list_to_integer([N1, N0]) of
        N when N =< 255 ->
            address_fast(Rest, Num bor (N bsl Shift), Shift - 8)
    end;
address_fast([N0, $. | Rest], Num, Shift) when Shift >= 8 ->
    case N0 - $0 of
        N when N =< 255 ->
            address_fast(Rest, Num bor (N bsl Shift), Shift - 8)
    end;
address_fast(L=[_N2, _N1, _N0], Num, 0) ->
    case list_to_integer(L) of
        N when N =< 255 ->
            Num bor N
    end;
address_fast(L=[_N1, _N0], Num, 0) ->
    case list_to_integer(L) of
        N when N =< 255 ->
            Num bor N
    end;
address_fast([N0], Num, 0) ->
    case N0 - $0 of
        N when N =< 255 ->
            Num bor N
    end.

%% @spec ip2long(Address) -> {ok, integer()}
%% @doc Convert an IP address from a string, IPv4 tuple or IPv6 tuple to the
%%      big endian integer representation.
ip2long(Address) when is_integer(Address) ->
    {ok, Address};
ip2long(Address) when is_list(Address) ->
    case catch address_fast(Address, 0, 24) of
        N when is_integer(N) ->
            {ok, N};
        _ ->
            case inet_parse:address(Address) of
                {ok, Tuple} ->
                    ip2long(Tuple);
                Error ->
                    Error
            end
    end;
ip2long({B3, B2, B1, B0}) ->
    {ok, (B3 bsl 24) bor (B2 bsl 16) bor (B1 bsl 8) bor B0};
ip2long({W7, W6, W5, W4, W3, W2, W1, W0}) ->
    {ok, (W7 bsl 112) bor (W6 bsl 96) bor (W5 bsl 80) bor (W4 bsl 64) bor
         (W3 bsl 48) bor (W2 bsl 32) bor (W1 bsl 16) bor W0};
ip2long(<<Addr:32>>) ->
    {ok, Addr};
ip2long(<<Addr:128>>) ->
    {ok, Addr};
ip2long(_) ->
    {error, badmatch}.

lookup_record(D, Ip) ->
    {ok, Bits, IPV} = geodata2_ip:make_ip(Ip),
    geodata2_format:lookup(D#geoipdb.meta, D#geoipdb.data, Bits, IPV).

priv_path(Components) ->
    AppDir = case code:which(?MODULE) of
                 cover_compiled -> "..";
                 F -> filename:dirname(filename:dirname(F))
             end,
    filename:join([AppDir, "priv" | Components]).
