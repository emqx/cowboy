%% Regression handler for emqx/emqx#19258.

-module(ws_linger_close_shutdown_h).
-behavior(cowboy_websocket_linger).

-export([init/2]).
-export([websocket_init/1]).
-export([websocket_close/2]).

init(Req, _) ->
	TestPid = list_to_pid(binary_to_list(cowboy_req:header(<<"x-test-pid">>, Req))),
	{cowboy_websocket_linger, Req, TestPid}.

websocket_init(TestPid) ->
	TestPid ! {?MODULE, self()},
	{ok, TestPid}.

websocket_close(Reason, TestPid) ->
	TestPid ! {?MODULE, {websocket_close, Reason}},
	{[{shutdown, Reason}], TestPid}.
