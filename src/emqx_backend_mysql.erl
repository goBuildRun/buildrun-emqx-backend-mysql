-file("/emqx_rel/_checkouts/emqx_backend_mysql/src/e"
      "mqx_backend_mysql.erl",
      1).

-module(emqx_backend_mysql).

-file("/emqx_rel/_checkouts/emqx_backend_mysql/inclu"
      "de/emqx_backend_mysql.hrl",
      1).

-file("/emqx_rel/_checkouts/emqx_backend_mysql/src/e"
      "mqx_backend_mysql.erl",
      10).

-file("/emqx_rel/_checkouts/emqx/include/emqx.hrl", 1).

-record(subscription, {topic, subid, subopts}).

-record(message,
	{id  :: binary(), qos = 0, from  :: atom() | binary(),
	 flags  :: #{atom() => boolean()}, headers  :: map(),
	 topic  :: binary(), payload  :: binary(),
	 timestamp  :: integer()}).

-record(delivery,
	{sender  :: pid(), message  :: #message{}}).

-record(route,
	{topic  :: binary(),
	 dest  :: node() | {binary(), node()}}).

-type trie_node_id() :: binary() | atom().

-record(trie_node,
	{node_id  :: trie_node_id(),
	 edge_count = 0  :: non_neg_integer(),
	 topic  :: binary() | undefined, flags  :: [atom()]}).

-record(trie_edge,
	{node_id  :: trie_node_id(),
	 word  :: binary() | atom()}).

-record(trie,
	{edge  :: #trie_edge{}, node_id  :: trie_node_id()}).

-record(alarm,
	{id  :: binary(),
	 severity  :: notice | warning | error | critical,
	 title  :: iolist(), summary  :: iolist(),
	 timestamp  :: erlang:timestamp()}).

-record(plugin,
	{name  :: atom(), dir  :: string(), descr  :: string(),
	 vendor  :: string(), active = false  :: boolean(),
	 info  :: map(), type  :: atom()}).

-record(command,
	{name  :: atom(), action  :: atom(),
	 args = []  :: list(), opts = []  :: list(),
	 usage  :: string(), descr  :: string()}).

-record(banned,
	{who  ::
	     {clientid, binary()} | {username, binary()} |
	     {ip_address, inet:ip_address()},
	 by  :: binary(), reason  :: binary(), at  :: integer(),
	 until  :: integer()}).

-file("/emqx_rel/_checkouts/emqx_backend_mysql/src/e"
      "mqx_backend_mysql.erl",
      12).

-export([pool_name/1]).

-export([register_metrics/0, load/0, unload/0]).

-export([on_client_connected/3, on_subscribe_lookup/3,
	 on_client_disconnected/4, on_message_fetch/4,
	 on_retain_lookup/4, on_message_publish/2,
	 on_message_store/2, on_message_retain/2,
	 on_retain_delete/2, on_message_delivered/3,
	 on_message_acked/3, run_mysql_sql/2, run_mysql_sql/3,
	 run_mysql_sql/4]).

pool_name(Pool) ->
    list_to_atom(lists:concat([emqx_backend_mysql, '_',
			       Pool])).

register_metrics() ->
    [emqx_metrics:new(MetricName)
     || MetricName
	    <- ['backend.mysql.client_connected',
		'backend.mysql.subscribe_lookup',
		'backend.mysql.client_disconnected',
		'backend.mysql.on_message_fetch',
		'backend.mysql.on_retain_lookup',
		'backend.mysql.on_message_publish',
		'backend.mysql.on_message_store',
		'backend.mysql.on_message_retain',
		'backend.mysql.on_retain_delete',
		'backend.mysql.on_message_acked',
		'backend.mysql.run_mysql_sql.publish',
		'backend.mysql.run_mysql_sql.acked_delivered',
		'backend.mysql.run_mysql_sql.sub_unsub',
		'backend.mysql.run_mysql_sql.connected',
		'backend.mysql.run_mysql_sql.disconnected']].

load() ->
    HookList =
	parse_hook(application:get_env(emqx_backend_mysql,
				       hooks, [])),
    lists:foreach(fun ({Hook, Action, Pool, Filter,
			OfflineOpts}) ->
			  case proplists:get_value(<<"function">>, Action) of
			    undefined ->
				SqlList = [compile_sql(SQL)
					   || SQL
						  <- proplists:get_value(<<"sql">>,
									 Action,
									 [])],
				load_(Hook, run_mysql_sql, OfflineOpts,
				      {Filter, Pool, SqlList});
			    Fun ->
				load_(Hook, b2a(Fun), OfflineOpts,
				      {Filter, Pool})
			  end
		  end,
		  HookList),
    io:format("~s is loaded.~n", [emqx_backend_mysql]),
    ok.

load_(Hook, Fun, OfflineOpts, Params) ->
    case Hook of
      'client.connected' ->
	  emqx:hook(Hook, fun emqx_backend_mysql:Fun/3, [Params]);
      'client.disconnected' ->
	  emqx:hook(Hook, fun emqx_backend_mysql:Fun/4, [Params]);
      'session.subscribed' ->
	  emqx:hook(Hook, fun emqx_backend_mysql:Fun/4,
		    [erlang:append_element(Params, OfflineOpts)]);
      'session.unsubscribed' ->
	  emqx:hook(Hook, fun emqx_backend_mysql:Fun/4, [Params]);
      'message.publish' ->
	  emqx:hook(Hook, fun emqx_backend_mysql:Fun/2, [Params]);
      'message.acked' ->
	  emqx:hook(Hook, fun emqx_backend_mysql:Fun/3, [Params]);
      'message.delivered' ->
	  emqx:hook(Hook, fun emqx_backend_mysql:Fun/3, [Params])
    end.

unload() ->
    HookList =
	parse_hook(application:get_env(emqx_backend_mysql,
				       hooks, [])),
    lists:foreach(fun ({Hook, Action, _Pool, _Filter,
			_OfflineOpts}) ->
			  case proplists:get_value(<<"function">>, Action) of
			    undefined -> unload_(Hook, run_mysql_sql);
			    Fun -> unload_(Hook, b2a(Fun))
			  end
		  end,
		  HookList),
    io:format("~s is unloaded.~n", [emqx_backend_mysql]),
    ok.

unload_(Hook, Fun) ->
    case Hook of
      'client.connected' ->
	  emqx:unhook(Hook, fun emqx_backend_mysql:Fun/3);
      'client.disconnected' ->
	  emqx:unhook(Hook, fun emqx_backend_mysql:Fun/4);
      'session.subscribed' ->
	  emqx:unhook(Hook, fun emqx_backend_mysql:Fun/4);
      'session.unsubscribed' ->
	  emqx:unhook(Hook, fun emqx_backend_mysql:Fun/4);
      'message.publish' ->
	  emqx:unhook(Hook, fun emqx_backend_mysql:Fun/2);
      'message.acked' ->
	  emqx:unhook(Hook, fun emqx_backend_mysql:Fun/3);
      'message.delivered' ->
	  emqx:unhook(Hook, fun emqx_backend_mysql:Fun/3)
    end.

on_client_connected(#{clientid := ClientId}, _ConnInfo,
		    {Filter, Pool}) ->
    with_filter(fun () ->
			emqx_metrics:inc('backend.mysql.client_connected'),
			emqx_backend_mysql_cli:client_connected(Pool,
								[{clientid,
								  ClientId}])
		end,
		undefined, Filter).

on_subscribe_lookup(#{clientid := ClientId}, _ConnInfo,
		    {Filter, Pool}) ->
    with_filter(fun () ->
			emqx_metrics:inc('backend.mysql.subscribe_lookup'),
			case emqx_backend_mysql_cli:subscribe_lookup(Pool,
								     [{clientid,
								       ClientId}])
			    of
			  [] -> ok;
			  TopicTable -> self() ! {subscribe, TopicTable}, ok
			end
		end,
		undefined, Filter).

on_client_disconnected(#{clientid := ClientId}, _Reason,
		       _ConnInfo, {Filter, Pool}) ->
    with_filter(fun () ->
			emqx_metrics:inc('backend.mysql.client_disconnected'),
			emqx_backend_mysql_cli:client_disconnected(Pool,
								   [{clientid,
								     ClientId}])
		end,
		undefined, Filter).

on_message_fetch(#{clientid := ClientId}, Topic, Opts,
		 {Filter, Pool, OfflineOpts}) ->
    with_filter(fun () ->
			emqx_metrics:inc('backend.mysql.on_message_fetch'),
			case maps:get(qos, Opts, 0) > 0 andalso
			       maps:get(first, Opts, true)
			    of
			  true ->
			      MsgList =
				  emqx_backend_mysql_cli:message_fetch(Pool,
								       [{clientid,
									 ClientId},
									{topic,
									 Topic}],
								       OfflineOpts),
			      [self() ! {deliver, Topic, Msg}
			       || Msg <- MsgList];
			  false -> ok
			end
		end,
		Topic, Filter).

on_retain_lookup(_Client, Topic, _Opts,
		 {Filter, Pool, _OfflineOpts}) ->
    with_filter(fun () ->
			emqx_metrics:inc('backend.mysql.on_retain_lookup'),
			MsgList = emqx_backend_mysql_cli:lookup_retain(Pool,
								       [{topic,
									 Topic}]),
			[self() ! {deliver, Topic, set_retain(true, Msg)}
			 || Msg <- MsgList]
		end,
		Topic, Filter).

set_retain(Value, Msg) ->
    Msg1 = emqx_message:set_flags(#{retained => Value},
				  Msg),
    emqx_message:set_header(retained, Value, Msg1).

on_message_publish(Msg = #message{flags =
				      #{retain := true},
				  payload = <<>>},
		   _Rule) ->
    {ok, Msg};
on_message_publish(Msg = #message{qos = Qos},
		   {_Filter, _Pool})
    when Qos =:= 0 ->
    {ok, Msg};
on_message_publish(Msg0 = #message{topic = Topic},
		   {Filter, Pool}) ->
    with_filter(fun () ->
			emqx_metrics:inc('backend.mysql.on_message_publish'),
			Msg = emqx_backend_mysql_cli:message_publish(Pool,
								     Msg0),
			{ok, Msg}
		end,
		Msg0, Topic, Filter).

on_message_store(Msg = #message{flags =
				    #{retain := true},
				payload = <<>>},
		 _Rule) ->
    {ok, Msg};
on_message_store(Msg0 = #message{topic = Topic},
		 {Filter, Pool}) ->
    with_filter(fun () ->
			emqx_metrics:inc('backend.mysql.on_message_store'),
			Msg = emqx_backend_mysql_cli:message_store(Pool, Msg0),
			{ok, Msg}
		end,
		Msg0, Topic, Filter).

on_message_retain(Msg = #message{flags =
				     #{retain := false}},
		  _Rule) ->
    {ok, Msg};
on_message_retain(Msg = #message{flags =
				     #{retain := true},
				 payload = <<>>},
		  _Rule) ->
    {ok, Msg};
on_message_retain(Msg0 = #message{flags =
				      #{retain := true},
				  topic = Topic, headers = Headers0},
		  {Filter, Pool}) ->
    Headers = case erlang:is_map(Headers0) of
		true -> Headers0;
		false -> #{}
	      end,
    case maps:find(retained, Headers) of
      {ok, true} -> {ok, Msg0};
      _ ->
	  with_filter(fun () ->
			      emqx_metrics:inc('backend.mysql.on_message_retain'),
			      Msg = emqx_backend_mysql_cli:message_retain(Pool,
									  Msg0),
			      {ok, Msg}
		      end,
		      Msg0, Topic, Filter)
    end;
on_message_retain(Msg, _Rule) -> {ok, Msg}.

on_retain_delete(Msg0 = #message{flags =
				     #{retain := true},
				 topic = Topic, payload = <<>>},
		 {Filter, Pool}) ->
    with_filter(fun () ->
			emqx_metrics:inc('backend.mysql.on_retain_delete'),
			Msg = emqx_backend_mysql_cli:delete_retain(Pool, Msg0),
			{ok, Msg}
		end,
		Msg0, Topic, Filter);
on_retain_delete(Msg, _Rule) -> {ok, Msg}.

on_message_delivered(_Client, _Msg, _Rule) -> ok.

on_message_acked(#{clientid := ClientId},
		 #message{topic = Topic, headers = Headers},
		 {Filter, Pool}) ->
    case maps:get(mysql_id, Headers, undefined) of
      undefined -> ok;
      Id ->
	  with_filter(fun () ->
			      emqx_metrics:inc('backend.mysql.on_message_acked'),
			      emqx_backend_mysql_cli:message_acked(Pool,
								   [{clientid,
								     ClientId},
								    {topic,
								     Topic},
								    {mysql_id,
								     Id}])
		      end,
		      Topic, Filter)
    end.

run_mysql_sql(Msg0 = #message{topic = Topic},
	      {Filter, Pool, SqlList}) ->
    with_filter(fun () ->
			emqx_metrics:inc('backend.mysql.run_mysql_sql.publish'),
			Msg = emqx_backend_mysql_cli:run_mysql_sql(Pool, Msg0,
								   SqlList),
			{ok, Msg}
		end,
		Msg0, Topic, Filter).

run_mysql_sql(#{clientid := ClientId},
	      #message{topic = Topic, id = MsgId},
	      {Filter, Pool, SqlList}) ->
    with_filter(fun () ->
			emqx_metrics:inc('backend.mysql.run_mysql_sql.acked_delivered'),
			emqx_backend_mysql_cli:run_mysql_sql(Pool,
							     [{clientid,
							       ClientId},
							      {topic, Topic},
							      {msgid, MsgId}],
							     SqlList)
		end,
		Topic, Filter);
run_mysql_sql(#{clientid := ClientId}, _ConnAttrs,
	      {Filter, Pool, SqlList}) ->
    with_filter(fun () ->
			emqx_metrics:inc('backend.mysql.run_mysql_sql.connected'),
			emqx_backend_mysql_cli:run_mysql_sql(Pool,
							     [{clientid,
							       ClientId}],
							     SqlList)
		end,
		undefined, Filter);
run_mysql_sql(_, _, _) -> ok.

run_mysql_sql(#{clientid := ClientId}, Topic, Opts,
	      {Filter, Pool, SqlList})
    when is_binary(Topic) ->
    with_filter(fun () ->
			emqx_metrics:inc('backend.mysql.run_mysql_sql.sub_unsub'),
			QoS = maps:get(qos, Opts, 0),
			emqx_backend_mysql_cli:run_mysql_sql(Pool,
							     [{clientid,
							       ClientId},
							      {topic, Topic},
							      {qos, QoS}],
							     SqlList)
		end,
		Topic, Filter);
run_mysql_sql(Client, Topic, Opts,
	      {Filter, Pool, SqlList, _})
    when is_binary(Topic) ->
    run_mysql_sql(Client, Topic, Opts,
		  {Filter, Pool, SqlList});
run_mysql_sql(#{clientid := ClientId}, _Reason,
	      _ConnInfo, {_Filter, Pool, SqlList}) ->
    emqx_metrics:inc('backend.mysql.run_mysql_sql.disconnected'),
    emqx_backend_mysql_cli:run_mysql_sql(Pool,
					 [{clientid, ClientId}], SqlList);
run_mysql_sql(_, _, _, _) -> ok.

parse_hook(Hooks) -> parse_hook(Hooks, []).

parse_hook([], Acc) -> Acc;
parse_hook([{Hook, Item} | Hooks], Acc) ->
    Params = emqx_json:decode(Item),
    Action = proplists:get_value(<<"action">>, Params),
    Pool = proplists:get_value(<<"pool">>, Params),
    Filter = proplists:get_value(<<"topic">>, Params),
    OfflineOpts =
	parse_offline_opts(proplists:get_value(<<"offline_opts">>,
					       Params, [])),
    parse_hook(Hooks,
	       [{l2a(Hook), Action, pool_name(b2a(Pool)), Filter,
		 OfflineOpts}
		| Acc]).

compile_sql(SQL) ->
    case re:run(SQL, <<"\\$\\{[^}]+\\}">>,
		[global, {capture, all, binary}])
	of
      nomatch -> {SQL, []};
      {match, Vars} ->
	  {re:replace(SQL, <<"\\$\\{[^}]+\\}">>, <<"?">>,
		      [global, {return, binary}]),
	   [Var || [Var] <- Vars]}
    end.

with_filter(Fun, _, undefined) -> Fun(), ok;
with_filter(Fun, Topic, Filter) ->
    case emqx_topic:match(Topic, Filter) of
      true -> Fun(), ok;
      false -> ok
    end.

with_filter(Fun, _, _, undefined) -> Fun();
with_filter(Fun, Msg, Topic, Filter) ->
    case emqx_topic:match(Topic, Filter) of
      true -> Fun();
      false -> {ok, Msg}
    end.

l2a(L) -> erlang:list_to_atom(L).

b2a(B) -> erlang:binary_to_atom(B, utf8).

b2l(B) -> erlang:binary_to_list(B).

parse_offline_opts(OfflineOpts) ->
    parse_offline_opts(OfflineOpts, []).

parse_offline_opts([], Acc) -> Acc;
parse_offline_opts([{<<"max_returned_count">>,
		     MaxReturnedCount}
		    | OfflineOpts],
		   Acc)
    when is_integer(MaxReturnedCount) ->
    parse_offline_opts(OfflineOpts,
		       [{max_returned_count, MaxReturnedCount} | Acc]);
parse_offline_opts([{<<"time_range">>, TimeRange}
		    | OfflineOpts],
		   Acc)
    when is_binary(TimeRange) ->
    parse_offline_opts(OfflineOpts,
		       [{time_range,
			 cuttlefish_duration:parse(b2l(TimeRange), s)}
			| Acc]);
parse_offline_opts([_ | OfflineOpts], Acc) ->
    parse_offline_opts(OfflineOpts, Acc).


