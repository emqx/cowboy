%% Feel free to use, reuse and abuse the code in this file.

-module(ws_send_many).

-export([init/2]).
-export([websocket_init/1]).
-export([websocket_handle/2]).
-export([websocket_info/2]).

init(Req, Opts) ->
	Sequence = proplists:get_value(sequence, Opts),
	{cowboy_test_ws:module(Opts), Req, Sequence}.

websocket_init(State) ->
	erlang:send_after(10, self(), send_many),
	{[], State}.

websocket_handle(_Frame, State) ->
	{[], State}.

websocket_info(send_many, Sequence) ->
	{Sequence, _State = Sequence}.
