%% This module sends an empty ping to the client and
%% waits for a pong before sending a text frame. It
%% is used to confirm server-initiated pings work.

-module(ws_linger_h).
-behavior(cowboy_websocket_linger).

-export([init/2]).
-export([websocket_init/1]).
-export([websocket_handle/2]).
-export([websocket_info/2]).
-export([websocket_close/2]).

init(Req, RunOrHibernate) ->
	TestPid = list_to_pid(binary_to_list(cowboy_req:header(<<"x-test-pid">>, Req))),
	{cowboy_websocket_linger, Req, {TestPid, RunOrHibernate}, #{
		active_n => 10
	}}.

websocket_init(State = {TestPid, RunOrHibernate}) ->
	TestPid ! {?MODULE, self()},
	case RunOrHibernate of
		run -> {ok, State};
		hibernate -> {ok, State, hibernate}
	end.

websocket_handle(_, State = {_, RunOrHibernate}) ->
	Command = {binary, <<"ACK">>},
	case RunOrHibernate of
		run -> {[Command], State};
		hibernate -> {[Command], State, hibernate}
	end.

websocket_info(thatsit, State = {_, RunOrHibernate}) ->
	Commands = [{shutdown, thatsit}],
	case RunOrHibernate of
		run -> {Commands, State};
		hibernate -> {Commands, State, hibernate}
	end.

websocket_close(Reason, State = {TestPid, _RunOrHibernate}) ->
	TestPid ! {?MODULE, {websocket_close, Reason}},
	{ok, State}.
