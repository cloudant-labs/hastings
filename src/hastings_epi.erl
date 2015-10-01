-module(hastings_epi).

-behaviour(couch_epi_plugin).

-export([
    app/0,
    providers/0,
    services/0,
    data_subscriptions/0,
    data_providers/0,
    processes/0,
    notify/3
]).

app() ->
    hastings.

providers() ->
    [
         {chttpd_handlers, hastings_httpd_handlers}
    ].


services() ->
    [].

data_subscriptions() ->
    [].

data_providers() ->
    [].

processes() ->
    [].

notify(_Key, _Old, _New) ->
    ok.
