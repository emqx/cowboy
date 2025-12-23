%% Feel free to use, reuse and abuse the code in this file.

-module(cowboy_test_ws).
-compile(export_all).
-compile(nowarn_export_all).

module(Opts) ->
    proplists:get_value(cowboy_ws_module, Opts, cowboy_websocket).

mkopts(Module) ->
	[{cowboy_ws_module, Module}].
