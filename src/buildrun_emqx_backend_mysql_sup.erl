%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(buildrun_emqx_backend_mysql_sup).

-include("buildrun_emqx_backend_mysql.hrl").

-behaviour(supervisor).


-export([start_link/1]).

-export([init/1]).

start_link(Pools) ->
    supervisor:start_link({local, buildrun_emqx_backend_mysql_sup}, 
    						buildrun_emqx_backend_mysql_sup, [Pools]).

init([Pools]) ->
    {ok,
     {{one_for_one, 10, 100},
      [pool_spec(Pool, Env) || {Pool, Env} <- Pools]}}.

pool_spec(Pool, Env) ->
    ecpool:pool_spec({buildrun_emqx_backend_mysql, Pool},
                     buildrun_emqx_backend_mysql:pool_name(Pool),
                     buildrun_emqx_backend_mysql_cli, Env).