# erlang环境配置

erlang 推荐使用 22 版本，经过测试用 23 版本编译出来运行有问题。各平台安装 erlang 详情请参考官方文档。 linux 系统可以使用 kerl 脚本安装，构建最新版本的 emqx 时注意编译的 gcc 版本不能用 10，编译通不过，推荐用 gcc-9

下载 kerl

``` bash
curl -O https://raw.githubusercontent.com/kerl/kerl/master/kerl
chmod +x kerl
```

安装 erlang

``` bash
kerl update releases
# 如果直接构建报错，修改 ~/.kerl/builds/22.3/otp_src_22.3/Makefile
# 将 gcc 改为 gcc9
kerl build 22.3 22.3
kerl install 22.3 /opt/erlang/22.3
# 启用环境
source /opt/erlang/22.3/activate
# 设置环境变量，这样不用每次手动启用
echo 'source /opt/erlang/22.3/activate' >> ~/.bashrc
```

安装 rebar3

``` bash
curl -OL https://s3.amazonaws.com/rebar3/rebar3
./rebar3 local install
# 然后根据输出操作
```

<a id="orgeb79a2a"></a>

