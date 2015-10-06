%% @author Bob Ippolito <bob@redivi.com>
%% @copyright 2006 Bob Ippolito

-module(egeoip_sup).
-author('bob@redivi.com').

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1, init_cluster/0, init_cluster/1]).
-export([worker/2, id/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->

    init_cluster(),

    {ConfigModule, ConfigFun} = case application:get_env(egeoip, config_interp) of
                                    {ok, {Cm, Cf}} -> {Cm, Cf};
                                    _              -> {?MODULE, id}
                                end,
    File = case application:get_env(egeoip, dbfile) of
        {ok, Other} ->
            ConfigModule:ConfigFun(Other);
        _ ->
            city
    end,
    Processes = worker(tuple_to_list(egeoip_cluster:worker_names()), File),
    {ok, {{one_for_one, 5, 300}, Processes}}.

worker([], _File) ->
    [];
worker([Name | T], File) ->

    [{Name,
      {egeoip, start_link, [Name, File]},
      permanent, 5000, worker, [egeoip]} | worker(T, File)].

init_cluster() ->
    init_cluster(10).

init_cluster(NumNodes) ->

    DynModuleBegin = "
        -module(egeoip_cluster).
        -export([worker_names/0, worker_count/0]).

        worker_count() -> ~p.

        worker_names() ->
            {egeoip_0\n",

    DynModuleMap = ", egeoip_~p\n",
    DynModuleEnd = "}.\n",

    Nodes = lists:seq(1, NumNodes),

    ModuleString = lists:flatten([
                io_lib:format(DynModuleBegin, [NumNodes]),
                lists:map(fun(I) -> io_lib:format(DynModuleMap, [I]) end, Nodes),
                DynModuleEnd
                ]),

    {M, B} = dynamic_compile:from_string(ModuleString),
    code:load_binary(M, "", B).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

id(X) -> X.
