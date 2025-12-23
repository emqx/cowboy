%% Feel free to use, reuse and abuse the code in this file.

-module(ws_timeout_cancel).

-export([init/2]).
-export([websocket_handle/2]).
-export([websocket_info/2]).

init(Req, Opts) ->
	erlang:start_timer(500, self(), should_not_cancel_timer),
	{cowboy_test_ws:module(Opts), Req, undefined, #{
		idle_timeout => 1000
	}}.

websocket_handle({text, Data}, State) ->
	{[{text, Data}], State};
websocket_handle({binary, Data}, State) ->
	{[{binary, Data}], State}.

websocket_info(_Info, State) ->
	erlang:start_timer(500, self(), should_not_cancel_timer),
	{[], State}.
