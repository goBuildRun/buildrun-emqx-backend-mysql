{application,emqx_backend_mysql,
             [{description,"EMQ X MySQL Backend"},
              {vsn,"4.1.1"},
              {modules,[emqx_backend_mysql,emqx_backend_mysql_actions,
                        emqx_backend_mysql_app,emqx_backend_mysql_batcher,
                        emqx_backend_mysql_cli,emqx_backend_mysql_sup]},
              {registered,[emqx_backend_mysql_sup]},
              {applications,[kernel,stdlib,mysql,ecpool]},
              {mod,{emqx_backend_mysql_app,[]}}]}.
