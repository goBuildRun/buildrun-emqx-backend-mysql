-file("/emqx_rel/_checkouts/emqx_backend_mysql/src/e"
      "mqx_backend_mysql_app.erl",
      1).

-module(emqx_backend_mysql_app).

-file("/emqx_rel/_checkouts/emqx_backend_mysql/inclu"
      "de/emqx_backend_mysql.hrl",
      1).

-file("/emqx_rel/_checkouts/emqx_backend_mysql/src/e"
      "mqx_backend_mysql_app.erl",
      10).

-behaviour(application).

-emqx_plugin(backend).

-export([start/2, stop/1]).

start(_Type, _Args) ->
    Pools = application:get_env(emqx_backend_mysql, pools,
				[]),
    {ok, Sup} = emqx_backend_mysql_sup:start_link(Pools),
    emqx_backend_mysql:register_metrics(),
    emqx_backend_mysql:load(),
    {ok, Sup}.

stop(_State) -> emqx_backend_mysql:unload().


