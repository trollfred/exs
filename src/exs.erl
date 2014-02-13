-module(exs).

-behavior(gen_server).

-include("exs.hrl").


-export([start/0,start/1]).
-export([stop/1]).
-export([directory/2]).
-export([read/2]).
-export([get_perms/2]).
-export([mkdir/2]).
-export([watch/2,watch/3]).
-export([unwatch/3]).

-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).




start() ->
    start(?XS_DEFAULT_PATH).

start(Path) ->
    gen_server:start_link(?MODULE, Path, []).

stop(Pid) ->
    gen_server:call(Pid, stop).

directory(Pid, Path) ->
    call(Pid, ?XS_DIRECTORY, Path).
    
read(Pid, Path) ->
    call(Pid, ?XS_READ, Path).

get_perms(Pid, Path) ->
    call(Pid, ?XS_GET_PERMS, Path).

mkdir(Pid, Path) ->
    call(Pid, ?XS_MKDIR, Path).

watch(Pid, Path) ->
    watch(Pid, Path, generate_token()).

watch(Pid, Path, Token) ->
    call(Pid, ?XS_WATCH, {Path, Token}).

unwatch(Pid, Path, Token) ->
    call(Pid, ?XS_UNWATCH, {Path, Token}).




init(Path) when is_list(Path) ->
    init(list_to_binary(Path));

init(Path) when is_binary(Path) ->
    Length = byte_size(Path),
    {ok, Socket} = procket:socket(1,1,0),
    ok = procket:connect(Socket, <<(procket:sockaddr_common(1, Length))/binary, Path/binary, 0:((procket:unix_path_max()-Length)*8)>>),
    Port = open_port({fd, Socket, Socket}, [binary, stream]),
    Tokens = ets:new(tokens,[]),
    {ok, #state{port = Port, fd = Socket, path = Path, tokens = Tokens}}.

handle_call({call, ?XS_WATCH, {Path, Token}}, {Client,_}, State) ->
    ok = request(State#state.port, ?XS_WATCH, Path++[0]++Token),
    true = ets:insert_new(State#state.tokens, {Token, Client}),
    {reply, {ok, Token}, State};

handle_call({call, ?XS_UNWATCH, {Path, Token}}, _From, State) ->
    case request(State#state.port, ?XS_UNWATCH, Path++[0]++Token) of
        true -> 
            ets:delete(State#state.tokens, Token),
            {reply, ok, State};
        {error, Error} ->
            {reply, {error, Error}, State}
    end;

handle_call({call, Command, Data}, _From, State) ->
    Answer = request(State#state.port, Command, Data),
    {reply, Answer, State};

handle_call(stop, _From, State) ->
    port_close(State#state.port),
    procket:close(State#state.fd),
    ets:delete(State#state.tokens),
    {stop, normal, ok, #state{path = State#state.path}}.

handle_cast(watch, State) ->
    {noreply, State}.

handle_info({Port, {data, <<?XS_WATCH_EVENT:32/little, _RxTx:64, Length:32/little, Payload:Length/binary>>}}, #state{port = Port} = State) ->
    process_event(Payload, State),    
    {noreply, State};

handle_info({Port, {data, <<?XS_WATCH_EVENT:32/little, _RxTx:64, Length:32/little, Payload:Length/binary, Rest/binary>>}}, #state{port = Port} = State) ->
    self() ! {Port, {data, Payload}},
    self() ! {Port, {data, Rest}},
    {noreply, State};

handle_info({Port, {data, <<?XS_WATCH_EVENT:32/little, _RxTx:64, Length:32/little, Part/binary>>}}, #state{port = Port} = State) when byte_size(Part) < Length ->
    process_event(collect_packet(Port, Part, Length - byte_size(Part)), State),
    {noreply, State};

handle_info({Port, {data, <<Beginning/binary>>}}, #state{port = Port} = State) ->
    receive
        {Port, {data, <<Rest/binary>>}} ->
            self() ! {Port, {data, <<Beginning/binary, Rest/binary>>}}
    after 1000 ->
        timeout
    end,
    {noreply, State}.

collect_packet(Port, Having, Needed) ->
    receive
        {Port, {data, <<Payload:Needed/binary>>}} ->
            <<Having/binary, Payload/binary>>;
        {Port, {data, <<Payload:Needed/binary, Rest/binary>>}} ->
            self() ! {Port, {data, Rest}},
            <<Having/binary, Payload/binary>>;
        {Port, {data, <<Payload/binary>>}} ->
            collect_packet(Port, <<Having/binary, Payload/binary>>, Needed - byte_size(Payload))
    after 1000 ->
        timeout
    end.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.




request(Port, Command, Data) ->
    Tx = 0,
    request(Port, Command, Tx, Data).

request(Port, Command, Tx, Data) ->
    Rx = erlang:round(random:uniform()*100500),
    request(Port, Command, Rx, Tx, Data).

request(Port, Command, Rx, Tx, Data) ->
    Length = string:len(Data) + 1,
    Payload = prepare_data(Data),
    Port ! {self(), {command, <<Command:32/little, Rx:32/little, Tx:32/little, Length:32/little, Payload/binary, 0>>}},
    parse_response(fetch_response(Port, Rx)).

fetch_response(Port, Rx) ->
    receive
        {Port, {data, <<Code:32/little, Rx:32/little, _Tx:32/little, Length:32/little, Payload:Length/binary>>}} ->
            {Code, Payload};
        {Port, {data, <<Code:32/little, Rx:32/little, _Tx:32/little, Length:32/little, Payload:Length/binary, Rest/binary>>}} ->
            self() ! {Port, {data, Rest}},
            {Code, Payload}
    after
        3000 ->
            {error, timeout, [{rx,Rx}]}
    end.

process_event(Payload, State) ->
    [Path, Token] = parse_response({?XS_WATCH_EVENT,Payload}),
    [[Client]] = ets:match(State#state.tokens, {Token,'$1'}),
    Client ! {exs,{event, Path}},
    ok.


%helpers

zero_split(Data) when is_binary(Data) ->
    List = binary:split(Data, <<0>>,[global,trim]),
    lists:map(fun (Element) -> binary_to_list(Element) end, List).

zero_untrail(Data) when is_binary(Data) ->
    [Value] = zero_split(Data),
    Value.

prepare_data(Data) when is_list(Data) ->
    list_to_binary(Data);
    
prepare_data(Data) when is_binary(Data) ->
    Data.

generate_token() ->
    binary_to_list(base64:encode(crypto:rand_bytes(10))).


% parsers

parse_response({?XS_DIRECTORY, Response}) ->
    zero_split(Response);

parse_response({?XS_READ, Response}) ->
    zero_untrail(Response);

parse_response({?XS_GET_PERMS, Response}) ->
    zero_split(Response);

parse_response({?XS_MKDIR, <<"OK",0>>}) ->
    ok;

parse_response({?XS_WATCH, <<"OK",0>>}) ->
    ok;

parse_response({?XS_UNWATCH, <<"OK",0>>}) ->
    ok;

parse_response({?XS_WATCH_EVENT, Response}) ->
    zero_split(Response);

parse_response({?XS_ERROR, Response}) ->
    {error, zero_untrail(Response)};

% fallback parser
parse_response({Code, Response}) ->
    {error, "unknown response code", {code, Code}, {response, Response}};

parse_response({error, Reason, Info}) ->
    {error, Reason, Info}.

call(Pid, Command, Data) ->
    gen_server:call(Pid, {call, Command, Data}, 4000).
