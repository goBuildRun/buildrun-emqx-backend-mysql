-file("/emqx_rel/_checkouts/emqx_backend_mysql/src/e"
      "mqx_backend_mysql_batcher.erl",
      1).

-module(emqx_backend_mysql_batcher).

-behaviour(gen_statem).

-export([start_link/3, call/2, call/3, get_cbst/1,
	 try_batch/3, set_value/3]).

-export([callback_mode/0, init/1]).

-export([do/3]).

callback_mode() -> [state_functions, state_enter].

start_link(InitFun, CallHandler, Opts) ->
    gen_statem:start_link(emqx_backend_mysql_batcher,
			  {InitFun, CallHandler, Opts}, []).

call(Pid, Data) -> gen_statem:call(Pid, {call, Data}).

call(Pid, Data, Opts) ->
    gen_statem:call(Pid, {call, Data, Opts}).

try_batch(Pid, Data, Fun) ->
    gen_statem:call(Pid, {try_batch, Data, Fun}).

set_value(Pid, Key, Value) ->
    gen_statem:cast(Pid, {set_value, Key, Value}).

get_cbst(Pid) -> gen_statem:call(Pid, get_cbst).

init({InitFun, CallHandler, Opts}) ->
    {ok, CbSt} = InitFun(),
    BatchSize = maps:get(batch_size, Opts, 100),
    BatchTime = maps:get(batch_time, Opts, 10),
    St = #{batch_size => BatchSize, batch_time => BatchTime,
	   handler => CallHandler, cb_st => CbSt, acc => [],
	   acc_left => BatchSize},
    {ok, do, St}.

do(enter, _, #{batch_time := T}) ->
    Action = {timeout, T, flush},
    {keep_state_and_data, [Action]};
do({call, From}, {call, Data, flush}, St) ->
    {repeat_state, flush(From, Data, St)};
do({call, From}, {call, Data},
   #{acc := Acc, acc_left := Left} = St0) ->
    St = St0#{acc := [{From, Data} | Acc],
	      acc_left := Left - 1},
    case Left =< 1 of
      true -> {repeat_state, flush(St)};
      false -> {repeat_state, St}
    end;
do({call, From}, {try_batch, {Ref, Data}, Fun}, St) ->
    case Fun() of
      true -> do({call, From}, {call, {i, Ref, Data}}, St);
      false ->
	  do({call, From}, {call, {Ref, Data}, flush}, St)
    end;
do(cast, {set_value, Key, Value}, St) ->
    put(Key, Value), {repeat_state, St};
do({call, From}, get_cbst, #{cb_st := CbSt}) ->
    {keep_state_and_data, [{reply, From, CbSt}]};
do(info, {timeout, _Ref, flush}, St) ->
    {repeat_state, flush(St)};
do(timeout, flush, St) -> {repeat_state, flush(St)}.

flush(#{acc := []} = St) -> St;
flush(#{acc := Acc, batch_size := Size, cb_st := CbSt,
	handler := Handler} =
	  St) ->
    {Froms, Batch} = lists:unzip(lists:reverse(Acc)),
    {Results, NewCbSt} = Handler(CbSt, Batch),
    lists:foreach(fun ({From, Result}) ->
			  gen_statem:reply(From, Result)
		  end,
		  lists:zip(Froms, Results)),
    St#{cb_st := NewCbSt, acc_left := Size, acc := []}.

flush(From, Data,
      #{cb_st := CbSt, handler := Handler} = St) ->
    {Result, NewCbSt} = Handler(CbSt, Data),
    gen_statem:reply(From, Result),
    St#{cb_st := NewCbSt}.


