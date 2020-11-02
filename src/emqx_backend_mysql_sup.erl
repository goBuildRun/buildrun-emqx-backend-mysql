-file("/emqx_rel/_checkouts/emqx_backend_mysql/src/e"
      "mqx_backend_mysql_sup.erl",
      1).

-module(emqx_backend_mysql_sup).

-file("/emqx_rel/_checkouts/emqx_backend_mysql/inclu"
      "de/emqx_backend_mysql.hrl",
      1).

-file("/emqx_rel/_checkouts/emqx_backend_mysql/src/e"
      "mqx_backend_mysql_sup.erl",
      10).

-behaviour(supervisor).

-export([start_link/1]).

-export([init/1]).

start_link(Pools) ->
    supervisor:start_link({local, emqx_backend_mysql_sup},
			  emqx_backend_mysql_sup, [Pools]).

init([Pools]) ->
    {ok,
     {{one_for_one, 10, 100},
      [pool_spec(Pool, Env) || {Pool, Env} <- Pools]}}.

pool_spec(Pool, Env) ->
    ecpool:pool_spec({emqx_backend_mysql, Pool},
		     emqx_backend_mysql:pool_name(Pool),
		     emqx_backend_mysql_cli, Env).


