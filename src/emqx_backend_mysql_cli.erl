-file("/emqx_rel/_checkouts/emqx_backend_mysql/src/e"
      "mqx_backend_mysql_cli.erl",
      1).

-module(emqx_backend_mysql_cli).

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
      "mqx_backend_mysql_cli.erl",
      10).

-export([client_connected/2, subscribe_lookup/2,
	 client_disconnected/2, message_fetch/3, lookup_retain/2,
	 message_publish/2, message_store/2, message_retain/2,
	 delete_retain/2, message_acked/2, run_mysql_sql/3]).

-export([record_to_msg/1]).

-export([connect/1, send_batch/2]).

client_connected(Pool, Msg) ->
    mysql_execute(Pool, client_connected_query,
		  [proplists:get_value(clientid, Msg), 1,
		   atom_to_list(node()), 1, atom_to_list(node())]),
    ok.

subscribe_lookup(Pool, Msg) ->
    case mysql_execute(Pool, subscribe_lookup_query,
		       feed_var([<<"${clientid}">>], Msg))
	of
      {ok, _Cols, []} -> [];
      {error, Error} ->
	  logger:error("Lookup retain error: ~p", [Error]), [];
      {ok, _Cols, Rows} ->
	  [{Topic, #{qos => Qos}} || [Topic, Qos] <- Rows]
    end.

client_disconnected(Pool, Msg) ->
    mysql_execute(Pool, client_disconnected_query,
		  [0, proplists:get_value(clientid, Msg)]),
    ok.

message_fetch(Pool, Msg, Opts) ->
    Topic = proplists:get_value(topic, Msg),
    ClientId = proplists:get_value(clientid, Msg),
    case mysql_execute(Pool, message_fetch_query,
		       feed_var([<<"${topic}">>, <<"${clientid}">>], Msg))
	of
      {ok, [_], []} ->
	  MsgId = case mysql_execute(Pool, message_lastid_query,
				     [Topic])
		      of
		    {ok, [_], []} -> 0;
		    {ok, [_], [[MId]]} -> MId;
		    {error, Error2} ->
			logger:error("Lookup msg error: ~p", [Error2]), 0
		  end,
	  mysql_execute(Pool, insert_acked_query,
			[ClientId, Topic, MsgId]),
	  [];
      {ok, [_], [[AckId]]} ->
	  Sql = <<"select id, topic, msgid, sender, qos, "
		  "payload, retain, UNIX_TIMESTAMP(arrived) "
		  "as arrived from mqtt_msg where id > "
		  "? and topic = ? ">>,
	  {Sql1, Params1} = case proplists:get_value(time_range,
						     Opts)
				of
			      undefined ->
				  {<<" order by id DESC ">>, [AckId, Topic]};
			      TimeRange ->
				  Time = erlang:system_time(seconds) -
					   TimeRange,
				  {<<" and arrived >= FROM_UNIXTIME(?) order "
				     "by id DESC ">>,
				   [AckId, Topic, Time]}
			    end,
	  {Sql2, Params2} = case
			      proplists:get_value(max_returned_count, Opts)
				of
			      undefined -> {<<"">>, Params1};
			      MaxReturnedCount ->
				  {<<" limit ?">>,
				   Params1 ++ [MaxReturnedCount]}
			    end,
	  case mysql_query(Pool,
			   <<Sql/binary, Sql1/binary, Sql2/binary>>, Params2)
	      of
	    {ok, _Cols, []} -> [];
	    {ok, Cols, Rows} ->
		[begin
		   M = record_to_msg(lists:zip(Cols, Row)),
		   M#message{id = emqx_guid:from_base62(M#message.id)}
		 end
		 || Row <- lists:reverse(Rows)];
	    {error, Error1} ->
		logger:error("Lookup msg error: ~p", [Error1]), []
	  end;
      {error, Error} ->
	  logger:error("Lookup msg error: ~p", [Error]), []
    end.

lookup_retain(Pool, Msg0) ->
    case mysql_execute(Pool, lookup_retain_query,
		       [proplists:get_value(topic, Msg0)])
	of
      {ok, Cols, [Row | _]} ->
	  M = record_to_msg(lists:zip(Cols, Row)),
	  [M#message{id = emqx_guid:from_base62(M#message.id)}];
      {ok, _Cols, []} -> [];
      {error, Error} ->
	  logger:error("Lookup retain error: ~p", [Error]), []
    end.

message_publish(Pool, Msg) ->
    ParamsKey = [<<"${msgid}">>, <<"${clientid}">>,
		 <<"${topic}">>, <<"${qos}">>, <<"${retain}">>,
		 <<"${payload}">>, <<"${timestamp}">>],
    case mysql_insert(Pool, message_publish_query,
		      feed_var(ParamsKey, Msg))
	of
      {ok, Id} -> emqx_message:set_header(mysql_id, Id, Msg);
      {error, Error} ->
	  logger:error("Failed to store message: ~p", [Error]),
	  Msg
    end.

message_store(Pool, Msg) ->
    case mysql_insert_batch(Pool, Msg) of
      {error, Error} ->
	  logger:error("Failed to store message: ~p", [Error]),
	  Msg;
      _ -> Msg
    end.

message_retain(Pool, Msg) ->
    ParamsKey = [<<"${topic}">>, <<"${msgid}">>,
		 <<"${clientid}">>, <<"${qos}">>, <<"${payload}">>,
		 <<"${timestamp}">>, <<"${msgid}">>, <<"${clientid}">>,
		 <<"${qos}">>, <<"${payload}">>, <<"${timestamp}">>],
    case mysql_execute(Pool, message_retain_query,
		       feed_var(ParamsKey, Msg))
	of
      ok -> Msg;
      {error, Error} ->
	  logger:error("Failed to retain message: ~p", [Error]),
	  Msg
    end.

delete_retain(Pool, Msg) ->
    case mysql_execute(Pool, delete_retain_query,
		       feed_var([<<"${topic}">>], Msg))
	of
      ok -> Msg;
      {error, Error} ->
	  logger:error("Failed to delete retain: ~p", [Error]),
	  Msg
    end.

message_acked(Pool, Msg) ->
    ParamsKey = [<<"${clientid}">>, <<"${topic}">>,
		 <<"${mysql_id}">>, <<"${mysql_id}">>],
    case mysql_execute(Pool, message_acked_query,
		       feed_var(ParamsKey, Msg))
	of
      ok -> ok;
      {error, Error} ->
	  logger:error("Failed to ack message: ~p", [Error])
    end.

run_mysql_sql(Pool, Msg, SqlList) ->
    lists:foreach(fun ({Sql, ParamsKey}) ->
			  case mysql_query(Pool, Sql, feed_var(ParamsKey, Msg))
			      of
			    ok -> ok;
			    {error, Error} ->
				logger:error("Sql:~p~n, params:~p, error: ~p",
					     [Sql, ParamsKey, Error])
			  end
		  end,
		  SqlList),
    Msg.

feed_var(Params, Msg) -> feed_var(Params, Msg, []).

feed_var([], _Msg, Acc) -> lists:reverse(Acc);
feed_var([<<"${topic}">> | Params],
	 Msg = #message{topic = Topic}, Acc) ->
    feed_var(Params, Msg, [Topic | Acc]);
feed_var([<<"${topic}">> | Params], Vals, Acc)
    when is_list(Vals) ->
    feed_var(Params, Vals,
	     [proplists:get_value(topic, Vals, null) | Acc]);
feed_var([<<"${msgid}">> | Params],
	 Msg = #message{id = MsgId}, Acc) ->
    feed_var(Params, Msg,
	     [emqx_guid:to_base62(MsgId) | Acc]);
feed_var([<<"${msgid}">> | Params], Vals, Acc)
    when is_list(Vals) ->
    feed_var(Params, Vals,
	     [emqx_guid:to_base62(proplists:get_value(msgid, Vals,
						      null))
	      | Acc]);
feed_var([<<"${mysql_id}">> | Params], Vals, Acc)
    when is_list(Vals) ->
    feed_var(Params, Vals,
	     [proplists:get_value(mysql_id, Vals, null) | Acc]);
feed_var([<<"${clientid}">> | Params],
	 Msg = #message{from = ClientId}, Acc)
    when is_atom(ClientId) ->
    feed_var(Params, Msg,
	     [atom_to_binary(ClientId, utf8) | Acc]);
feed_var([<<"${clientid}">> | Params],
	 Msg = #message{from = ClientId}, Acc)
    when is_binary(ClientId) ->
    feed_var(Params, Msg, [ClientId | Acc]);
feed_var([<<"${clientid}">> | Params], Vals, Acc)
    when is_list(Vals) ->
    feed_var(Params, Vals,
	     [proplists:get_value(clientid, Vals, null) | Acc]);
feed_var([<<"${qos}">> | Params],
	 Msg = #message{qos = Qos}, Acc) ->
    feed_var(Params, Msg, [Qos | Acc]);
feed_var([<<"${qos}">> | Params], Vals, Acc)
    when is_list(Vals) ->
    feed_var(Params, Vals,
	     [proplists:get_value(qos, Vals, null) | Acc]);
feed_var([<<"${retain}">> | Params],
	 Msg = #message{flags = #{retain := Retain}}, Acc) ->
    feed_var(Params, Msg, [i(Retain) | Acc]);
feed_var([<<"${payload}">> | Params],
	 Msg = #message{payload = Payload}, Acc) ->
    feed_var(Params, Msg, [Payload | Acc]);
feed_var([<<"${timestamp}">> | Params],
	 Msg = #message{timestamp = Ts}, Acc) ->
    feed_var(Params, Msg, [round(Ts / 1000) | Acc]);
feed_var([<<"${timestamp_str}">> | Params],
	 Msg = #message{timestamp = Ts}, Acc) ->
    {{Y, M, D}, {H, Mm, S}} =
	calendar:system_time_to_local_time(Ts, millisecond),
    TsStr =
	iolist_to_binary(io_lib:format("~w-~2.2.0w-~2.2.0w ~2.2.0w:~2.2.0w:~2.2.0w",
				       [Y, M, D, H, Mm, S])),
    feed_var(Params, Msg, [TsStr | Acc]);
feed_var([_ | Params], Msg, Acc) ->
    feed_var(Params, Msg, [null | Acc]).

i(true) -> 1;
i(false) -> 0.

b(0) -> false;
b(1) -> true.

record_to_msg(Record) ->
    record_to_msg(Record, #message{headers = #{}}).

record_to_msg([], Msg) -> Msg;
record_to_msg([{<<"id">>, Id} | Record], Msg) ->
    record_to_msg(Record,
		  emqx_message:set_header(mysql_id, Id, Msg));
record_to_msg([{<<"msgid">>, MsgId} | Record], Msg) ->
    record_to_msg(Record, Msg#message{id = MsgId});
record_to_msg([{<<"topic">>, Topic} | Record], Msg) ->
    record_to_msg(Record, Msg#message{topic = Topic});
record_to_msg([{<<"sender">>, Sender} | Record], Msg) ->
    record_to_msg(Record, Msg#message{from = Sender});
record_to_msg([{<<"qos">>, Qos} | Record], Msg) ->
    record_to_msg(Record, Msg#message{qos = Qos});
record_to_msg([{<<"retain">>, R} | Record], Msg) ->
    record_to_msg(Record,
		  Msg#message{flags = #{retain => b(R)}});
record_to_msg([{<<"payload">>, Payload} | Record],
	      Msg) ->
    record_to_msg(Record, Msg#message{payload = Payload});
record_to_msg([{<<"arrived">>, Arrived} | Record],
	      Msg) ->
    record_to_msg(Record, Msg#message{timestamp = Arrived});
record_to_msg([_ | Record], Msg) ->
    record_to_msg(Record, Msg).

connect(Options) ->
    Prepares = [{client_connected_query,
		 <<"insert into mqtt_client(clientid, state, "
		   "node, online_at, offline_at) values(?, "
		   "?, ?, now(), null) on duplicate key "
		   "update state = ?, node = ?, online_at "
		   "= now(), offline_at = null">>},
		{subscribe_lookup_query,
		 <<"select topic, qos from mqtt_sub where "
		   "clientid = ?">>},
		{client_disconnected_query,
		 <<"update mqtt_client set state = ?, offline_at "
		   "= now() where clientid = ?">>},
		{message_fetch_query,
		 <<"select mid from mqtt_acked where topic "
		   "= ? and clientid = ?">>},
		{message_lastid_query,
		 <<"select id from mqtt_msg where topic "
		   "= ? order by id DESC limit 1">>},
		{insert_acked_query,
		 <<"insert into mqtt_acked(clientid, topic, "
		   "mid) values(?, ?, ?)">>},
		{lookup_retain_query,
		 <<"select topic, msgid, sender, qos, payload, "
		   "UNIX_TIMESTAMP(arrived) as arrived from "
		   "mqtt_retain where topic = ?">>},
		{message_publish_query,
		 <<"insert into mqtt_msg(msgid, sender, "
		   "topic, qos, retain, payload, arrived) "
		   "values (?, ?, ?, ?, ?, ?, FROM_UNIXTIME(?))">>},
		{message_retain_query,
		 <<"insert into mqtt_retain(topic, msgid, "
		   "sender, qos, payload, arrived) values "
		   "(?, ?, ?, ?, ?, FROM_UNIXTIME(?))on "
		   "duplicate key  update msgid = ?, sender "
		   "= ?, qos = ?, payload = ?, arrived = "
		   "FROM_UNIXTIME(?)">>},
		{delete_retain_query,
		 <<"delete from mqtt_retain where topic "
		   "= ?">>},
		{message_acked_query,
		 <<"insert into mqtt_acked(clientid, topic, "
		   "mid) values(?, ?, ?)  on duplicate key "
		   "update mid = ?">>}],
    InitFun = fun () ->
		      {ok, Conn} = mysql:start_link([{prepare, Prepares}
						     | Options]),
		      put(backslash_escapes_enabled,
			  gen_server:call(Conn, backslash_escapes_enabled)),
		      {ok, Conn}
	      end,
    Handler = fun (Conn, Batch) ->
		      {emqx_backend_mysql_cli:send_batch(Conn, Batch), Conn}
	      end,
    emqx_backend_mysql_batcher:start_link(InitFun, Handler,
					  #{}).

mysql_insert(Pool, Ref, Params) ->
    logger:debug("Pool:~p, Statement: ~p, Params:~p",
		 [Pool, Ref, Params]),
    ecpool:with_client(Pool,
		       fun (C) ->
			       emqx_backend_mysql_batcher:call(C,
							       {insert_returning_id,
								Ref, Params},
							       flush)
		       end).

mysql_query(Pool, Sql, Params) ->
    logger:debug("Pool:~p, SQL: ~p, Params:~p",
		 [Pool, Sql, Params]),
    ecpool:with_client(Pool,
		       fun (C) ->
			       emqx_backend_mysql_batcher:call(C,
							       {query, Sql,
								Params},
							       flush)
		       end).

mysql_execute(Pool, Ref, Params) ->
    logger:debug("Pool:~p, Statement: ~p, Params:~p",
		 [Pool, Ref, Params]),
    ecpool:with_client(Pool,
		       fun (C) ->
			       emqx_backend_mysql_batcher:call(C,
							       {execute, Ref,
								Params},
							       flush)
		       end).

mysql_insert_batch(Pool, Msg) ->
    logger:debug("Pool:~p, batch insert Msg: ~p",
		 [Pool, Msg]),
    ecpool:with_client(Pool,
		       fun (C) -> emqx_backend_mysql_batcher:call(C, Msg) end).

send_batch(Conn, {What, Sql, Params}) ->
    send_one(What, Conn, Sql, Params);
send_batch(Conn, Batch) -> send_inserts(Conn, Batch).

send_one(insert_returning_id, Conn, Sql, Params) ->
    case mysql:execute(Conn, Sql, Params) of
      ok -> {ok, mysql:insert_id(Conn)};
      {error, Error} -> {error, Error}
    end;
send_one(query, Conn, Sql, Params) ->
    mysql:query(Conn, Sql, Params);
send_one(execute, Conn, Sql, Params) ->
    mysql:execute(Conn, Sql, Params).

send_inserts(_Conn, []) -> [];
send_inserts(Conn, Msgs) ->
    SQL = insert_msgs_sql(Msgs),
    mysql:query(Conn, SQL),
    lists:map(fun (_) -> ok end, Msgs).

insert_msgs_sql(Msgs) ->
    ParamsKey = [<<"${msgid}">>, <<"${clientid}">>,
		 <<"${topic}">>, <<"${qos}">>, <<"${retain}">>,
		 <<"${payload}">>, <<"${timestamp_str}">>],
    Values = lists:map(fun (Msg) ->
			       lists:map(fun quote/1, feed_var(ParamsKey, Msg))
		       end,
		       Msgs),
    ["insert into mqtt_msg(msgid, sender, "
     "topic, qos, retain, payload, arrived) ",
     "values ", values(Values)].

values([]) -> [];
values([Columns | Rest]) ->
    ["(", infix(Columns, ", "), ")",
     case Rest of
       [] -> "";
       _ -> ", "
     end
     | values(Rest)].

infix([X], _) -> [X];
infix([H | T], In) -> [H, In | infix(T, In)].

quote(V) when is_integer(V) -> integer_to_list(V);
quote(V) when is_atom(V) -> atom_to_binary(V, utf8);
quote(V) when is_list(V) ->
    quote(unicode:characters_to_binary(V));
quote(V) when is_binary(V) -> quote_str(V).

quote_str(Bin0) when is_binary(Bin0) ->
    Bin = case get(backslash_escapes_enabled) of
	    true ->
		binary:replace(Bin0, <<"\\">>, <<"\\\\">>, [global]);
	    false -> Bin0
	  end,
    Escaped = binary:replace(Bin, <<"'">>, <<"''">>,
			     [global]),
    [$', Escaped, $'].


