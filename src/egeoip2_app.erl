%% @author Bob Ippolito <bob@redivi.com>
%% @copyright 2006 Bob Ippolito

-module(egeoip2_app).
-author('bob@redivi.com').

-behaviour(application).
-export([start/2, stop/1]).

start(_Type, _StartArgs) ->
    egeoip2_sup:start_link().

stop(_State) ->
    ok.
