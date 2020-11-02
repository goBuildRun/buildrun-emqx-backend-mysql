-file("/emqx_rel/_checkouts/emqx_backend_mysql/src/e"
      "mqx_backend_mysql_actions.erl",
      1).

-module(emqx_backend_mysql_actions).

-export(['$logger_header'/0]).

-behaviour(ecpool_worker).

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
      "mqx_backend_mysql_actions.erl",
      12).

-file("/emqx_rel/_checkouts/emqx/include/logger.hrl",
      1).

-file("/emqx_rel/_checkouts/emqx_backend_mysql/src/e"
      "mqx_backend_mysql_actions.erl",
      13).

-export([connect/1, send_batch/2]).

-import(emqx_rule_utils, [str/1, unsafe_atom_key/1]).

-export([on_resource_create/2, on_resource_destroy/2,
	 on_get_resource_status/2]).

-export([on_action_create_data_to_mysql/2]).

-export([on_action_create_offline_msg/2]).

-export([on_action_create_lookup_sub/2]).

-resource_type(#{create => on_resource_create,
		 description =>
		     #{en =>
			   <<77, 121, 83, 81, 76, 32, 68, 97, 116, 97, 98, 97, 115, 101>>,
		       zh =>
			   <<77, 121, 83, 81, 76, 32, 230, 149, 176, 230, 141, 174, 229, 186, 147>>},
		 destroy => on_resource_destroy, name => backend_mysql,
		 params =>
		     #{auto_reconnect =>
			   #{default => true,
			     description =>
				 #{en =>
				       <<73, 102, 32, 114, 101, 45, 116, 114,
					 121, 32, 119, 104, 101, 110, 32, 116,
					 104, 101, 32, 99, 111, 110, 110, 101,
					 99, 116, 105, 111, 110, 32, 108, 111,
					 115, 116>>,
				   zh =>
				       <<77, 121, 83, 81, 76, 32, 232, 191, 158,
					 230, 142, 165, 230, 150, 173, 229, 188,
					 128, 230, 151, 182, 230, 152, 175, 229,
					 144, 166, 233, 135, 141, 232, 191,
					 158>>},
			     order => 9,
			     title =>
				 #{en =>
				       <<69, 110, 97, 98, 108, 101, 32, 82, 101,
					 99, 111, 110, 110, 101, 99, 116>>,
				   zh =>
				       <<230, 152, 175, 229, 144, 166, 233, 135,
					 141, 232, 191, 158>>},
			     type => boolean},
		       batch_size =>
			   #{default => 100,
			     description =>
				 #{en =>
				       <<84, 104, 101, 32, 115, 105, 122, 101,
					 32, 111, 102, 32, 77, 121, 83, 81, 76,
					 32, 98, 97, 116, 99, 104, 32, 105, 110,
					 115, 101, 114, 116, 32, 83, 81, 76>>,
				   zh =>
				       <<77, 121, 83, 81, 76, 32, 230, 137, 185,
					 233, 135, 143, 229, 134, 153, 229, 133,
					 165, 229, 164, 167, 229, 176, 143>>},
			     order => 7,
			     title =>
				 #{en =>
				       <<66, 97, 116, 99, 104, 32, 83, 105, 122,
					 101>>,
				   zh =>
				       <<230, 137, 185, 233, 135, 143, 229, 134,
					 153, 229, 133, 165, 229, 164, 167, 229,
					 176, 143>>},
			     type => number},
		       batch_time =>
			   #{default => 10,
			     description =>
				 #{en =>
				       <<84, 105, 109, 101, 32, 111, 102, 32,
					 77, 121, 83, 81, 76, 32, 98, 97, 116,
					 99, 104, 32, 105, 110, 115, 101, 114,
					 116, 32, 83, 81, 76, 32, 105, 110, 116,
					 101, 114, 118, 97, 108, 40, 109, 105,
					 108, 108, 105, 115, 101, 99, 111, 110,
					 100, 41>>,
				   zh =>
				       <<77, 121, 83, 81, 76, 32, 230, 137, 185,
					 233, 135, 143, 229, 134, 153, 229, 133,
					 165, 233, 151, 180, 233, 154, 148, 40,
					 230, 175, 171, 231, 167, 146, 41>>},
			     order => 8,
			     title =>
				 #{en =>
				       <<66, 97, 116, 99, 104, 32, 84, 105, 109,
					 101>>,
				   zh =>
				       <<230, 137, 185, 233, 135, 143, 229, 134,
					 153, 229, 133, 165, 233, 151, 180, 233,
					 154, 148, 40, 230, 175, 171, 231, 167,
					 146, 41>>},
			     type => number},
		       database =>
			   #{description =>
				 #{en =>
				       <<68, 97, 116, 97, 98, 97, 115, 101, 32,
					 110, 97, 109, 101, 32, 102, 111, 114,
					 32, 99, 111, 110, 110, 101, 99, 116,
					 105, 110, 103, 32, 116, 111, 32, 77,
					 121, 83, 81, 76>>,
				   zh =>
				       <<77, 121, 83, 81, 76, 32, 230, 149, 176,
					 230, 141, 174, 229, 186, 147, 229, 144,
					 141>>},
			     order => 3, required => true,
			     title =>
				 #{en =>
				       <<77, 121, 83, 81, 76, 32, 68, 97, 116,
					 97, 98, 97, 115, 101>>,
				   zh =>
				       <<77, 121, 83, 81, 76, 32, 230, 149, 176,
					 230, 141, 174, 229, 186, 147, 229, 144,
					 141>>},
			     type => string},
		       password =>
			   #{default => <<>>,
			     description =>
				 #{en =>
				       <<80, 97, 115, 115, 119, 111, 114, 100,
					 32, 102, 111, 114, 32, 99, 111, 110,
					 110, 101, 99, 116, 105, 110, 103, 32,
					 116, 111, 32, 77, 121, 83, 81, 76>>,
				   zh =>
				       <<77, 121, 83, 81, 76, 32, 229, 175, 134,
					 231, 160, 129>>},
			     order => 6,
			     title =>
				 #{en =>
				       <<77, 121, 83, 81, 76, 32, 80, 97, 115,
					 115, 119, 111, 114, 100>>,
				   zh =>
				       <<77, 121, 83, 81, 76, 32, 229, 175, 134,
					 231, 160, 129>>},
			     type => string},
		       pool_size =>
			   #{default => 8,
			     description =>
				 #{en =>
				       <<84, 104, 101, 32, 115, 105, 122, 101,
					 32, 111, 102, 32, 99, 111, 110, 110,
					 101, 99, 116, 105, 111, 110, 32, 112,
					 111, 111, 108, 32, 102, 111, 114, 32,
					 77, 121, 83, 81, 76>>,
				   zh =>
				       <<77, 121, 83, 81, 76, 32, 232, 191, 158,
					 230, 142, 165, 230, 177, 160, 229, 164,
					 167, 229, 176, 143>>},
			     order => 4,
			     title =>
				 #{en =>
				       <<80, 111, 111, 108, 32, 83, 105, 122,
					 101>>,
				   zh =>
				       <<232, 191, 158, 230, 142, 165, 230, 177,
					 160, 229, 164, 167, 229, 176, 143>>},
			     type => number},
		       server =>
			   #{default =>
				 <<49, 50, 55, 46, 48, 46, 48, 46, 49, 58, 51,
				   51, 48, 54>>,
			     description =>
				 #{en =>
				       <<77, 121, 83, 81, 76, 32, 73, 80, 32,
					 97, 100, 100, 114, 101, 115, 115, 32,
					 111, 114, 32, 104, 111, 115, 116, 110,
					 97, 109, 101, 32, 97, 110, 100, 32,
					 112, 111, 114, 116>>,
				   zh =>
				       <<77, 121, 83, 81, 76, 32, 230, 156, 141,
					 229, 138, 161, 229, 153, 168, 229, 156,
					 176, 229, 157, 128>>},
			     order => 1, required => true,
			     title =>
				 #{en =>
				       <<77, 121, 83, 81, 76, 32, 83, 101, 114,
					 118, 101, 114>>,
				   zh =>
				       <<77, 121, 83, 81, 76, 32, 230, 156, 141,
					 229, 138, 161, 229, 153, 168>>},
			     type => string},
		       user =>
			   #{description =>
				 #{en =>
				       <<85, 115, 101, 114, 110, 97, 109, 101,
					 32, 102, 111, 114, 32, 99, 111, 110,
					 110, 101, 99, 116, 105, 110, 103, 32,
					 116, 111, 32, 77, 121, 83, 81, 76>>,
				   zh =>
				       <<77, 121, 83, 81, 76, 32, 231, 148, 168,
					 230, 136, 183, 229, 144, 141>>},
			     order => 5, required => true,
			     title =>
				 #{en =>
				       <<77, 121, 83, 81, 76, 32, 85, 115, 101,
					 114, 32, 78, 97, 109, 101>>,
				   zh =>
				       <<77, 121, 83, 81, 76, 32, 231, 148, 168,
					 230, 136, 183, 229, 144, 141>>},
			     type => string}},
		 status => on_get_resource_status,
		 title =>
		     #{en => <<77, 121, 83, 81, 76>>,
		       zh => <<77, 121, 83, 81, 76>>}}).

-rule_action(#{create => on_action_create_data_to_mysql,
	       description =>
		   #{en =>
			 <<68, 97, 116, 97, 32, 116, 111, 32, 77, 121, 83, 81,
			   76>>,
		     zh =>
			 <<228, 191, 157, 229, 173, 152, 230, 149, 176, 230,
			   141, 174, 229, 136, 176, 32, 77, 121, 83, 81, 76, 32,
			   230, 149, 176, 230, 141, 174, 229, 186, 147>>},
	       for => '$any', name => data_to_mysql,
	       params =>
		   #{'$resource' =>
			 #{description =>
			       #{en =>
				     <<66, 105, 110, 100, 32, 97, 32, 114, 101,
				       115, 111, 117, 114, 99, 101, 32, 116,
				       111, 32, 116, 104, 105, 115, 32, 97, 99,
				       116, 105, 111, 110>>,
				 zh =>
				     <<231, 187, 153, 229, 138, 168, 228, 189,
				       156, 231, 187, 145, 229, 174, 154, 228,
				       184, 128, 228, 184, 170, 232, 181, 132,
				       230, 186, 144>>},
			   required => true,
			   title =>
			       #{en =>
				     <<82, 101, 115, 111, 117, 114, 99, 101, 32,
				       73, 68>>,
				 zh =>
				     <<232, 181, 132, 230, 186, 144, 32, 73,
				       68>>},
			   type => string},
		     sql =>
			 #{description =>
			       #{en =>
				     <<83, 81, 76, 32, 116, 101, 109, 112, 108,
				       97, 116, 101, 32, 119, 105, 116, 104, 32,
				       112, 108, 97, 99, 101, 104, 111, 108,
				       100, 101, 114, 115, 32, 102, 111, 114,
				       32, 105, 110, 115, 101, 114, 116, 105,
				       110, 103, 47, 117, 112, 100, 97, 116,
				       105, 110, 103, 32, 100, 97, 116, 97, 32,
				       116, 111, 32, 77, 121, 83, 81, 76>>,
				 zh =>
				     <<229, 140, 133, 229, 144, 171, 228, 186,
				       134, 229, 141, 160, 228, 189, 141, 231,
				       172, 166, 231, 154, 132, 32, 83, 81, 76,
				       32, 230, 168, 161, 230, 157, 191, 239,
				       188, 140, 231, 148, 168, 228, 187, 165,
				       230, 143, 146, 229, 133, 165, 230, 136,
				       150, 230, 155, 180, 230, 150, 176, 230,
				       149, 176, 230, 141, 174, 229, 136, 176,
				       32, 77, 121, 83, 81, 76, 32, 230, 149,
				       176, 230, 141, 174, 229, 186, 147>>},
			   input => textarea, required => true,
			   title =>
			       #{en =>
				     <<83, 81, 76, 32, 84, 101, 109, 112, 108,
				       97, 116, 101>>,
				 zh =>
				     <<83, 81, 76, 32, 230, 168, 161, 230, 157,
				       191>>},
			   type => string}},
	       title =>
		   #{en =>
			 <<68, 97, 116, 97, 32, 116, 111, 32, 77, 121, 83, 81,
			   76>>,
		     zh =>
			 <<228, 191, 157, 229, 173, 152, 230, 149, 176, 230,
			   141, 174, 229, 136, 176, 32, 77, 121, 83, 81, 76>>},
	       types => [backend_mysql]}).

-rule_action(#{create => on_action_create_offline_msg,
	       description =>
		   #{en =>
			 <<79, 102, 102, 108, 105, 110, 101, 32, 77, 115, 103,
			   32, 116, 111, 32, 77, 121, 83, 81, 76>>,
		     zh =>
			 <<231, 166, 187, 231, 186, 191, 230, 182, 136, 230,
			   129, 175, 228, 191, 157, 229, 173, 152, 229, 136,
			   176, 32, 77, 121, 83, 81, 76>>},
	       for => '$any', name => offline_msg_to_mysql,
	       params =>
		   #{'$resource' =>
			 #{description =>
			       #{en =>
				     <<66, 105, 110, 100, 32, 97, 32, 114, 101,
				       115, 111, 117, 114, 99, 101, 32, 116,
				       111, 32, 116, 104, 105, 115, 32, 97, 99,
				       116, 105, 111, 110>>,
				 zh =>
				     <<231, 187, 153, 229, 138, 168, 228, 189,
				       156, 231, 187, 145, 229, 174, 154, 228,
				       184, 128, 228, 184, 170, 232, 181, 132,
				       230, 186, 144>>},
			   required => true,
			   title =>
			       #{en =>
				     <<82, 101, 115, 111, 117, 114, 99, 101, 32,
				       73, 68>>,
				 zh =>
				     <<232, 181, 132, 230, 186, 144, 32, 73,
				       68>>},
			   type => string},
		     max_returned_count =>
			 #{default => 0,
			   description =>
			       #{en =>
				     <<77, 97, 120, 32, 110, 117, 109, 98, 101,
				       114, 32, 111, 102, 32, 102, 101, 116, 99,
				       104, 32, 111, 102, 102, 108, 105, 110,
				       101, 32, 109, 101, 115, 115, 97, 103,
				       101, 115>>,
				 zh =>
				     <<232, 142, 183, 229, 143, 150, 231, 166,
				       187, 231, 186, 191, 230, 182, 136, 230,
				       129, 175, 231, 154, 132, 230, 156, 128,
				       229, 164, 167, 230, 157, 161, 230, 149,
				       176>>},
			   required => false,
			   title =>
			       #{en =>
				     <<77, 97, 120, 32, 82, 101, 116, 117, 114,
				       110, 101, 100, 32, 67, 111, 117, 110,
				       116>>,
				 zh =>
				     <<77, 97, 120, 32, 82, 101, 116, 117, 114,
				       110, 101, 100, 32, 67, 111, 117, 110,
				       116>>},
			   type => number},
		     time_range =>
			 #{default => <<>>,
			   description =>
			       #{en =>
				     <<84, 105, 109, 101, 32, 82, 97, 110, 103,
				       101, 32, 111, 102, 32, 102, 101, 116, 99,
				       104, 32, 111, 102, 102, 108, 105, 110,
				       101, 32, 109, 101, 115, 115, 97, 103,
				       101, 115>>,
				 zh =>
				     <<232, 142, 183, 229, 143, 150, 230, 156,
				       128, 232, 191, 145, 230, 151, 182, 233,
				       151, 180, 232, 140, 131, 229, 155, 180,
				       229, 134, 133, 231, 154, 132, 231, 166,
				       187, 231, 186, 191, 230, 182, 136, 230,
				       129, 175>>},
			   required => false,
			   title =>
			       #{en =>
				     <<84, 105, 109, 101, 32, 82, 97, 110, 103,
				       101>>,
				 zh =>
				     <<84, 105, 109, 101, 32, 82, 97, 110, 103,
				       101>>},
			   type => string}},
	       title =>
		   #{en =>
			 <<79, 102, 102, 108, 105, 110, 101, 32, 77, 115, 103,
			   32, 116, 111, 32, 77, 121, 83, 81, 76>>,
		     zh =>
			 <<231, 166, 187, 231, 186, 191, 230, 182, 136, 230,
			   129, 175, 228, 191, 157, 229, 173, 152, 229, 136,
			   176, 32, 77, 121, 83, 81, 76>>},
	       types => [backend_mysql]}).

-rule_action(#{create => on_action_create_lookup_sub,
	       description =>
		   #{en =>
			 <<71, 101, 116, 32, 83, 117, 98, 115, 99, 114, 105,
			   112, 116, 105, 111, 110, 32, 76, 105, 115, 116, 32,
			   70, 114, 111, 109, 32, 77, 121, 83, 81, 76>>,
		     zh =>
			 <<228, 187, 142, 32, 77, 121, 83, 81, 76, 32, 228, 184,
			   173, 232, 142, 183, 229, 143, 150, 232, 174, 162,
			   233, 152, 133, 229, 136, 151, 232, 161, 168>>},
	       for => '$any', name => lookup_sub_to_mysql,
	       params =>
		   #{'$resource' =>
			 #{description =>
			       #{en =>
				     <<66, 105, 110, 100, 32, 97, 32, 114, 101,
				       115, 111, 117, 114, 99, 101, 32, 116,
				       111, 32, 116, 104, 105, 115, 32, 97, 99,
				       116, 105, 111, 110>>,
				 zh =>
				     <<231, 187, 153, 229, 138, 168, 228, 189,
				       156, 231, 187, 145, 229, 174, 154, 228,
				       184, 128, 228, 184, 170, 232, 181, 132,
				       230, 186, 144>>},
			   required => true,
			   title =>
			       #{en =>
				     <<82, 101, 115, 111, 117, 114, 99, 101, 32,
				       73, 68>>,
				 zh =>
				     <<232, 181, 132, 230, 186, 144, 32, 73,
				       68>>},
			   type => string}},
	       title =>
		   #{en =>
			 <<83, 117, 98, 115, 99, 114, 105, 112, 116, 105, 111,
			   110, 32, 76, 105, 115, 116, 32, 70, 114, 111, 109,
			   32, 77, 121, 83, 81, 76>>,
		     zh =>
			 <<228, 187, 142, 32, 77, 121, 83, 81, 76, 32, 228, 184,
			   173, 232, 142, 183, 229, 143, 150, 232, 174, 162,
			   233, 152, 133, 229, 136, 151, 232, 161, 168>>},
	       types => [backend_mysql]}).

on_resource_create(ResId,
		   Config = #{<<"server">> := Server, <<"user">> := User,
			      <<"database">> := DB}) ->
    begin
      logger:log(info, #{},
		 #{report_cb =>
		       fun (_) ->
			       {'$logger_header'() ++
				  "Initiating Resource ~p, ResId: ~p",
				[backend_mysql, ResId]}
		       end,
		   mfa =>
		       {emqx_backend_mysql_actions, on_resource_create, 2},
		   line => 229})
    end,
    {ok, _} = application:ensure_all_started(ecpool),
    {Host, Port} = host(Server),
    Options = [{host, str(Host)}, {port, Port},
	       {user, str(User)},
	       {password, str(maps:get(<<"password">>, Config, ""))},
	       {database, str(DB)},
	       {auto_reconnect,
		case maps:get(<<"auto_reconnect">>, Config, true) of
		  true -> 15;
		  false -> false
		end},
	       {pool_size, maps:get(<<"pool_size">>, Config, 8)},
	       {batch_size, maps:get(<<"batch_size">>, Config, 100)},
	       {batch_time, maps:get(<<"batch_time">>, Config, 10)}],
    PoolName = pool_name(ResId),
    start_resource(ResId, PoolName, Options),
    #{<<"pool">> => PoolName}.

start_resource(ResId, PoolName, Options) ->
    case ecpool:start_sup_pool(PoolName,
			       emqx_backend_mysql_actions, Options)
	of
      {ok, _} ->
	  begin
	    logger:log(info, #{},
		       #{report_cb =>
			     fun (_) ->
				     {'$logger_header'() ++
					"Initiated Resource ~p Successfully, "
					"ResId: ~p",
				      [backend_mysql, ResId]}
			     end,
			 mfa => {emqx_backend_mysql_actions, start_resource, 3},
			 line => 251})
	  end;
      {error, {already_started, _Pid}} ->
	  on_resource_destroy(ResId, #{<<"pool">> => PoolName}),
	  start_resource(ResId, PoolName, Options);
      {error, Reason} ->
	  begin
	    logger:log(error, #{},
		       #{report_cb =>
			     fun (_) ->
				     {'$logger_header'() ++
					"Initiate Resource ~p failed, ResId: "
					"~p, ~0p",
				      [backend_mysql, ResId, Reason]}
			     end,
			 mfa => {emqx_backend_mysql_actions, start_resource, 3},
			 line => 256})
	  end,
	  error({{backend_mysql, ResId}, create_failed})
    end.

-spec on_get_resource_status(ResId :: binary(),
			     Params :: map()) -> Status :: map().

on_get_resource_status(_ResId,
		       #{<<"pool">> := PoolName}) ->
    Status = [ecpool_worker:is_connected(Worker)
	      || {_WorkerName, Worker} <- ecpool:workers(PoolName)],
    #{is_alive =>
	  length(Status) > 0 andalso
	    lists:all(fun (St) -> St =:= true end, Status)}.

on_resource_destroy(ResId, #{<<"pool">> := PoolName}) ->
    begin
      logger:log(info, #{},
		 #{report_cb =>
		       fun (_) ->
			       {'$logger_header'() ++
				  "Destroying Resource ~p, ResId: ~p",
				[backend_mysql, ResId]}
		       end,
		   mfa =>
		       {emqx_backend_mysql_actions, on_resource_destroy, 2},
		   line => 267})
    end,
    case ecpool:stop_sup_pool(PoolName) of
      ok ->
	  begin
	    logger:log(info, #{},
		       #{report_cb =>
			     fun (_) ->
				     {'$logger_header'() ++
					"Destroyed Resource ~p Successfully, "
					"ResId: ~p",
				      [backend_mysql, ResId]}
			     end,
			 mfa =>
			     {emqx_backend_mysql_actions, on_resource_destroy,
			      2},
			 line => 270})
	  end;
      {error, Reason} ->
	  begin
	    logger:log(error, #{},
		       #{report_cb =>
			     fun (_) ->
				     {'$logger_header'() ++
					"Destroy Resource ~p failed, ResId: ~p, ~p",
				      [backend_mysql, ResId, Reason]}
			     end,
			 mfa =>
			     {emqx_backend_mysql_actions, on_resource_destroy,
			      2},
			 line => 272})
	  end,
	  error({{backend_mysql, ResId}, destroy_failed})
    end.

-spec on_action_create_data_to_mysql(ActionInstId ::
					 binary(),
				     #{}) -> fun((Msg :: map()) -> any()).

on_action_create_data_to_mysql(ActionInstId,
			       #{<<"pool">> := PoolName,
				 <<"sql">> := SqlTemplate}) ->
    PrepareSqlKey = unsafe_atom_key(ActionInstId),
    begin
      logger:log(info, #{},
		 #{report_cb =>
		       fun (_) ->
			       {'$logger_header'() ++
				  "Initiating Action ~p, SqlTemplate: ~p, "
				  "PrepareSqlKey: ~p",
				[on_action_create_data_to_mysql, SqlTemplate,
				 PrepareSqlKey]}
		       end,
		   mfa =>
		       {emqx_backend_mysql_actions,
			on_action_create_data_to_mysql, 2},
		   line => 285})
    end,
    {PrepareStatement, GetPrepareParams} =
	emqx_rule_utils:preproc_sql(SqlTemplate, '?'),
    prepare_sql(PrepareSqlKey, PrepareStatement, PoolName),
    ecpool:add_reconnect_callback(PoolName,
				  fun (Batch) ->
					  prepare_sql_to_conn(PrepareSqlKey,
							      PrepareStatement,
							      emqx_backend_mysql_batcher:get_cbst(Batch))
				  end),
    fun (Msg, _Envs) ->
	    case mysql_query(PoolName, PrepareSqlKey,
			     PrepareStatement, GetPrepareParams(Msg))
		of
	      {error, Reason} -> error({data_to_mysql, Reason});
	      _ -> ok
	    end
    end.

on_action_create_offline_msg(ActionInstId,
			     #{<<"pool">> := PoolName,
			       <<"time_range">> := TimeRange0,
			       <<"max_returned_count">> :=
				   MaxReturnedCount0}) ->
    InsertSql = <<"insert into mqtt_msg(msgid, sender, "
		  "topic, qos, retain, payload, arrived) "
		  "values (?, ?, ?, ?, ?, ?, FROM_UNIXTIME(?) "
		  ")">>,
    DeleteSql = <<"delete from mqtt_msg where msgid = ? "
		  "and topic = ?">>,
    SelectSql = <<"select id, topic, msgid, sender, qos, "
		  "payload, retain, UNIX_TIMESTAMP(arrived) "
		  "from mqtt_msg where topic = ? order "
		  "by id DESC">>,
    InsertKey = unsafe_atom_key(<<"insert_",
				  ActionInstId/binary>>),
    DeleteKey = unsafe_atom_key(<<"delete_",
				  ActionInstId/binary>>),
    SelectKey = unsafe_atom_key(<<"select_",
				  ActionInstId/binary>>),
    TimeRange = case to_undefined(TimeRange0) of
		  undefined -> undefined;
		  TimeRange0 -> cuttlefish_duration:parse(TimeRange0, s)
		end,
    MaxReturnedCount = to_undefined(MaxReturnedCount0),
    {SelectSql1, Params} = case {TimeRange,
				 MaxReturnedCount}
			       of
			     {undefined, undefined} -> {SelectSql, []};
			     {TimeRange, undefined} ->
				 {<<SelectSql/binary,
				    " and arrived >= FROM_UNIXTIME(?) ">>,
				  [TimeRange div 1000]};
			     {undefined, MaxReturnedCount} ->
				 {<<SelectSql/binary, " limit ? ">>,
				  [MaxReturnedCount]};
			     {TimeRange, MaxReturnedCount} ->
				 {<<SelectSql/binary,
				    "  and arrived >= FROM_UNIXTIME(?) limit ? ">>,
				  [TimeRange div 1000, MaxReturnedCount]}
			   end,
    prepare_sql(InsertKey, InsertSql, PoolName),
    prepare_sql(DeleteKey, DeleteSql, PoolName),
    prepare_sql(SelectKey, SelectSql1, PoolName),
    ecpool:add_reconnect_callback(PoolName,
				  fun (Conn) ->
					  prepare_sql_to_conn(InsertKey,
							      InsertSql, Conn),
					  prepare_sql_to_conn(SelectKey,
							      SelectSql1, Conn),
					  prepare_sql_to_conn(DeleteKey,
							      DeleteSql, Conn)
				  end),
    fun (Msg = #{event := Event, topic := Topic}, _Envs) ->
	    case Event of
	      'message.publish' ->
		  #{id := MsgId, qos := Qos, flags := Flags,
		    payload := Payload, publish_received_at := Ts,
		    clientid := From} =
		      Msg,
		  Retain = maps:get(retain, Flags, true),
		  insert_message(PoolName, InsertKey, InsertSql,
				 [MsgId, From, Topic, Qos, i(Retain), Payload,
				  Ts div 1000]);
	      'session.subscribed' ->
		  MsgList = lookup_message(PoolName, SelectKey, SelectSql,
					   [Topic] ++ Params),
		  [self() ! {deliver, Topic, M} || M <- MsgList];
	      'message.acked' ->
		  #{id := MsgId} = Msg,
		  delete_message(PoolName, DeleteKey, DeleteSql,
				 [MsgId, Topic])
	    end
    end.

on_action_create_lookup_sub(ActionInstId,
			    #{<<"pool">> := PoolName}) ->
    SelectPrepare =
	<<"select topic, qos from mqtt_sub where "
	  "clientid = ?">>,
    InsertPrepare =
	<<"insert into mqtt_sub(clientid, topic, "
	  "qos) values(?, ?, ?)">>,
    SelectKey = unsafe_atom_key(<<"select_",
				  ActionInstId/binary>>),
    InsertKey = unsafe_atom_key(<<"insert_",
				  ActionInstId/binary>>),
    prepare_sql(SelectKey, SelectPrepare, PoolName),
    prepare_sql(InsertKey, InsertPrepare, PoolName),
    ecpool:add_reconnect_callback(PoolName,
				  fun (Batch) ->
					  prepare_sql_to_conn(SelectKey,
							      SelectPrepare,
							      emqx_backend_mysql_batcher:get_cbst(Batch)),
					  prepare_sql_to_conn(InsertKey,
							      InsertPrepare,
							      emqx_backend_mysql_batcher:get_cbst(Batch))
				  end),
    fun (Msg = #{event := Event, clientid := ClientId},
	 _Envs) ->
	    case Event of
	      'client.connected' ->
		  case lookup_subscribe(PoolName, SelectKey,
					SelectPrepare, [ClientId])
		      of
		    [] -> ok;
		    TopicTable -> self() ! {subscribe, TopicTable}
		  end;
	      'session.subscribed' ->
		  #{topic := Topic, qos := QoS} = Msg,
		  insert_subscribe(PoolName, InsertKey, InsertPrepare,
				   [ClientId, Topic, QoS])
	    end
    end.

prepare_sql(PrepareSqlKey, PrepareStatement,
	    PoolName) ->
    [begin
       {ok, Batch} = ecpool_worker:client(Worker),
       prepare_sql_to_conn(PrepareSqlKey, PrepareStatement,
			   emqx_backend_mysql_batcher:get_cbst(Batch)),
       case binary:split(PrepareStatement, <<"values ">>) of
	 [Insert, Params] ->
	     emqx_backend_mysql_batcher:set_value(Batch,
						  {insert_sql, PrepareSqlKey},
						  {binary_to_list(Insert),
						   binary_to_list(Params)});
	 _ -> ok
       end
     end
     || {_WorkerName, Worker} <- ecpool:workers(PoolName)].

prepare_sql_to_conn(PrepareSqlKey, PrepareStatement,
		    Conn) ->
    case mysql:prepare(Conn, PrepareSqlKey,
		       PrepareStatement)
	of
      {ok, Name} ->
	  begin
	    logger:log(info, #{},
		       #{report_cb =>
			     fun (_) ->
				     {'$logger_header'() ++
					"Prepare Statement: ~p, PreparedSQL Key: ~p",
				      [PrepareStatement, Name]}
			     end,
			 mfa =>
			     {emqx_backend_mysql_actions, prepare_sql_to_conn,
			      3},
			 line => 398})
	  end;
      {error, Reason} ->
	  begin
	    logger:log(error, #{},
		       #{report_cb =>
			     fun (_) ->
				     {'$logger_header'() ++
					"Prepare Statement: ~p, ~0p",
				      [PrepareStatement, Reason]}
			     end,
			 mfa =>
			     {emqx_backend_mysql_actions, prepare_sql_to_conn,
			      3},
			 line => 400})
	  end,
	  error(prepare_sql_failed)
    end.

connect(Options) ->
    InitFun = fun () ->
		      {ok, Conn} = mysql:start_link(Options),
		      put(backslash_escapes_enabled,
			  gen_server:call(Conn, backslash_escapes_enabled)),
		      {ok, Conn}
	      end,
    Handler = fun (Conn, Batch) ->
		      {emqx_backend_mysql_actions:send_batch(Conn, Batch),
		       Conn}
	      end,
    emqx_backend_mysql_batcher:start_link(InitFun, Handler,
					  #{batch_size =>
						proplists:get_value(batch_size,
								    Options),
					    batch_time =>
						proplists:get_value(batch_time,
								    Options)}).

mysql_query(Pool, Ref, PrepareStatement, Params) ->
    begin
      logger:log(debug, #{},
		 #{report_cb =>
		       fun (_) ->
			       {'$logger_header'() ++
				  "Send to mysql, pool: ~p, prepared_key: "
				  "~p, ~0p",
				[Pool, Ref, Params]}
		       end,
		   mfa => {emqx_backend_mysql_actions, mysql_query, 4},
		   line => 420})
    end,
    ecpool:with_client(Pool,
		       fun (Conn) ->
			       Fun = fun () ->
					     get({insert_sql, Ref}) =/=
					       undefined
				     end,
			       case emqx_backend_mysql_batcher:try_batch(Conn,
									 {Ref,
									  Params},
									 Fun)
				   of
				 {error, disconnected} -> exit(Conn, restart);
				 {error, not_prepared} ->
				     prepare_sql_to_conn(Ref, PrepareStatement,
							 emqx_backend_mysql_batcher:get_cbst(Conn)),
				     emqx_backend_mysql_batcher:try_batch(Conn,
									  {Ref,
									   Params},
									  Fun);
				 Res -> Res
			       end
		       end).

mysql_query_not_batch(Pool, Ref, PrepareStatement,
		      Params) ->
    begin
      logger:log(debug, #{},
		 #{report_cb =>
		       fun (_) ->
			       {'$logger_header'() ++
				  "Send to mysql, pool: ~p, prepared_key: "
				  "~p, ~0p",
				[Pool, Ref, Params]}
		       end,
		   mfa =>
		       {emqx_backend_mysql_actions, mysql_query_not_batch, 4},
		   line => 434})
    end,
    ecpool:with_client(Pool,
		       fun (Conn) ->
			       MConn =
				   emqx_backend_mysql_batcher:get_cbst(Conn),
			       case mysql:execute(MConn, Ref, Params) of
				 {error, disconnected} -> exit(Conn, restart);
				 {error, not_prepared} ->
				     prepare_sql_to_conn(Ref, PrepareStatement,
							 MConn),
				     mysql:execute(MConn, Ref, Params);
				 Res -> Res
			       end
		       end).

send_batch(Conn, {Ref, Params}) ->
    mysql:execute(Conn, Ref, Params);
send_batch(Conn, Batch) -> send_inserts(Conn, Batch).

send_inserts(Conn, Msgs) ->
    Msgs1 = maps:to_list(lists:foldl(fun ({_, K, V}, Acc) ->
					     L = maps:get(K, Acc, []),
					     Acc#{K => [V | L]}
				     end,
				     #{}, Msgs)),
    lists:map(fun ({Ref, Msgs2}) ->
		      SQL = insert_batch_sql(Ref, Msgs),
		      case mysql:query(Conn, SQL, lists:flatten(Msgs2)) of
			{error, Reason} -> error(insert_error, Reason);
			_ -> lists:map(fun (_) -> ok end, Msgs)
		      end
	      end,
	      Msgs1).

insert_batch_sql(Ref, Msgs) ->
    {InsertSQL, ValuesSQL} = get({insert_sql, Ref}),
    [InsertSQL, "values ", values(Msgs, ValuesSQL)].

values(Msgs, ValuesSQL) -> values(Msgs, ValuesSQL, []).

values([], _, Acc) -> Acc;
values([_ | Rest], ValuesSQL, Acc) ->
    NewAcc = case Rest == [] of
	       true -> Acc ++ ValuesSQL;
	       false -> Acc ++ ValuesSQL ++ ", "
	     end,
    values(Rest, ValuesSQL, NewAcc).

pool_name(ResId) ->
    list_to_atom("backend_mysql:" ++ str(ResId)).

host(Server) when is_binary(Server) ->
    case string:split(Server, ":") of
      [Host, Port] -> {Host, binary_to_integer(Port)};
      [Host] -> {Host, 3306}
    end.

insert_message(Pool, PrepareSqlKey, PrepareStatement,
	       PrepareParams) ->
    case mysql_query_not_batch(Pool, PrepareSqlKey,
			       PrepareStatement, PrepareParams)
	of
      {error, Error} ->
	  begin
	    logger:log(error, #{},
		       #{report_cb =>
			     fun (_) ->
				     {'$logger_header'() ++
					"Failed to store message: ~p",
				      [Error]}
			     end,
			 mfa => {emqx_backend_mysql_actions, insert_message, 4},
			 line => 492})
	  end;
      _ -> ok
    end.

lookup_message(Pool, PrepareSqlKey, PrepareStatement,
	       PrepareParams) ->
    case mysql_query_not_batch(Pool, PrepareSqlKey,
			       PrepareStatement, PrepareParams)
	of
      {ok, _Cols, []} -> [];
      {ok, Cols, Rows} ->
	  [begin
	     Msg =
		 emqx_backend_mysql_cli:record_to_msg(lists:zip(Cols,
								Row)),
	     Msg#message{id = emqx_guid:from_hexstr(Msg#message.id)}
	   end
	   || Row <- lists:reverse(Rows)];
      {error, Error} ->
	  begin
	    logger:log(error, #{},
		       #{report_cb =>
			     fun (_) ->
				     {'$logger_header'() ++
					"Failed to lookup message error: ~p",
				      [Error]}
			     end,
			 mfa => {emqx_backend_mysql_actions, lookup_message, 4},
			 line => 503})
	  end,
	  []
    end.

delete_message(Pool, PrepareSqlKey, PrepareStatement,
	       PrepareParams) ->
    case mysql_query_not_batch(Pool, PrepareSqlKey,
			       PrepareStatement, PrepareParams)
	of
      {error, Error} ->
	  begin
	    logger:log(error, #{},
		       #{report_cb =>
			     fun (_) ->
				     {'$logger_header'() ++
					"Failed to delete message: ~p",
				      [Error]}
			     end,
			 mfa => {emqx_backend_mysql_actions, delete_message, 4},
			 line => 508})
	  end;
      _ -> ok
    end.

insert_subscribe(Pool, PrepareSqlKey, PrepareStatement,
		 PrepareParams) ->
    case mysql_query_not_batch(Pool, PrepareSqlKey,
			       PrepareStatement, PrepareParams)
	of
      {error, Error} ->
	  begin
	    logger:log(error, #{},
		       #{report_cb =>
			     fun (_) ->
				     {'$logger_header'() ++
					"Failed to store subscribe: ~p",
				      [Error]}
			     end,
			 mfa =>
			     {emqx_backend_mysql_actions, insert_subscribe, 4},
			 line => 514})
	  end;
      _ -> ok
    end.

lookup_subscribe(Pool, PrepareSqlKey, PrepareStatement,
		 PrepareParams) ->
    case mysql_query_not_batch(Pool, PrepareSqlKey,
			       PrepareStatement, PrepareParams)
	of
      {ok, _Cols, []} -> [];
      {error, Error} ->
	  begin
	    logger:log(error, #{},
		       #{report_cb =>
			     fun (_) ->
				     {'$logger_header'() ++
					"Lookup subscription error: ~p",
				      [Error]}
			     end,
			 mfa =>
			     {emqx_backend_mysql_actions, lookup_subscribe, 4},
			 line => 521})
	  end,
	  [];
      {ok, _Cols, Rows} ->
	  [{Topic, #{qos => Qos}} || [Topic, Qos] <- Rows]
    end.

i(true) -> 1;
i(false) -> 0.

to_undefined(<<>>) -> undefined;
to_undefined(0) -> undefined;
to_undefined(V) -> V.

'$logger_header'() -> "[RuleEngine MySql] ".


