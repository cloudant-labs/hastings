%% Copyright 2015 IBM Cloudant

-module(hastings_httpd_handlers).

-export([url_handler/1, db_handler/1, design_handler/1]).

url_handler(_) -> no_match.

db_handler(<<"_geo_cleanup">>)  -> fun hastings_httpd:handle_cleanup_req/2;
db_handler(_) -> no_match.

design_handler(<<"_geo">>)      -> fun hastings_httpd:handle_search_req/3;
design_handler(<<"_geo_info">>) -> fun hastings_httpd:handle_info_req/3;
design_handler(_) -> no_match.
