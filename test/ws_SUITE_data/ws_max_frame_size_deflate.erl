-module(ws_max_frame_size_deflate).

-export([init/2]).
-export([websocket_handle/2]).
-export([websocket_info/2]).

init(Req, State) ->
	%% The message size limit is chosen by the test through the query string.
	MaxFrameSize = case cowboy_req:qs(Req) of
		<<"infinity">> -> infinity;
		_ -> 100
	end,
	{cowboy_test_ws:module(State), Req, State, #{
		max_frame_size => MaxFrameSize,
		compress => true
	}}.

%% The replies are sent uncompressed so that the tests only have to check the
%% payload the connection delivers, not the way it compresses its output.
websocket_handle({text, Data}, State) ->
	{[{deflate, false}, {text, Data}], State};
websocket_handle({binary, Data}, State) ->
	{[{deflate, false}, {binary, Data}], State};
websocket_handle(_Frame, State) ->
	{[], State}.

websocket_info(_Info, State) ->
	{[], State}.
