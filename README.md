# buildrun-emqx-backend-mysql
EMQX  client connection and message save to MySQL

emqx 客户端连接状态和消息持久化到MySQL 插件

### erlang 环境配置

若没配置erlang环境，请参考[erlang 环境配置](./erlang环境配置.md).

### 编译发布插件

1、clone emqx-rel 项目, 切换到改 tag  v4.1.2

``` bash
git clone https://github.com/emqx/emqx-rel.git
cd emqx-rel
git checkout v4.1.2
make
```

正常构建成功后，运行 `_build/emqx/rel/emqx/bin/emqx console` 启动 emqx

### 添加自定义插件

2.rebar.config 添加依赖

```erl
{deps,
   [ {buildrun_emqx_backend_mysql, {git, "https://github.com/goBuildRun/buildrun-emqx-backend-mysql.git", {branch, "master"}}}
   , ....
   ....
   ]
}

```

3.rebar.config 中 relx 段落添加

```erl
{relx,
    [...
    , ...
    , {release, {emqx, git_describe},
       [
         {buildrun_emqx_backend_mysql, load},
       ]
      }
    ]
}
```
4.编译

> make

打开 `localhost:18083` 的插件栏可以看到 `buildrun_emqx_backend_mysql` 插件。

### config配置

File: etc/buildrun_emqx_backend_mysql.conf

```
# mysql 服务器
mysql.server = 127.0.0.1:3306

# 连接池数量
mysql.pool_size = 8

# mysql 用户名
mysql.username = buildrun

# mysql密码
mysql.password = buildrun

# 数据库名
mysql.database = mqtt

# 超时时间（秒）
mysql.query_timeout = 10s

```

### mqtt_client.sql, mqtt_msg.sql

到你的数据库中执行[mqtt_client.sql](./sql/mqtt_client.sql), [mqtt_msg.sql](./sql/mqtt_msg.sql)。

### 加载插件

> ./bin/emqx_ctl plugins load buildrun_emqx_backend_mysql

或者编辑

data/loaded_plugins

> 添加 {buildrun_emqx_backend_mysql, true}.

设置插件自动启动

注意：这种方式适用emqx未启动之前

或者

可以在emqx启动后，在 dashboard 插件栏点击启用 `buildrun_emqx_backend_mysql` 即可启用成功

### 使用

此插件会把public发布的消息保存到mysql中，但并不是全部。也可以在业务规则中添加存储类型的资源。

需要在发布消息的参数中 retain 值设置为 true。 这样这条消息才会被保存在mysql中

eg：

```json
{
  "topic": "buildruntopic",
  "payload": "hello,Buidrun",
  "qos": 1,
  "retain": true,
  "client_id": "mqttjs_8b3b4182ae"
}
```

### 构建镜像

`emqx-rel` 项目自带构建镜像的功能

``` bash
make emqx-docker-build
```

### 最后

有什么问题和功能需求都可以给我提issue，欢迎关注。

### License

Apache License Version 2.0
