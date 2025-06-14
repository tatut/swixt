:- module(swixt, [q/1, q/2, insert/2, delete/2, status/1, tx/2]).
:- use_module(xtdb_mapping, [json_prolog/2, to_json/2, string_datetimetz/2]).
:- use_module(library(odbc)).
:- use_module(library(yall)).
:- use_module(library(apply)).
:- set_prolog_flag(xt_connection_info, "host=localhost port=5433 dbname=xtdb").
:- set_prolog_flag(xt_debug, false).
:- use_foreign_library(foreign(swixt)).

% Convert from Prolog term into a Oid-String representation for postgres API
xt_type(true, 16-true).
xt_type(false, 16-false).
xt_type(time(H,M,S,Micros), 1083-Str) :- format(string(Str), '~|~`0t~d~2+:~|~`0t~d~2+:~|~`0t~d~2+.~d', [H,M,S,Micros]).
xt_type(date(Y,M,D), 1082-Str) :- format(string(Str), '~|~`0t~d~4+-~|~`0t~d~2+-~|~`0t~d~2+', [Y,M,D]).
xt_type(X, 701-X) :- float(X).
xt_type(X, 20-X) :- integer(X).
xt_type(X, 25-X) :- string(X).
xt_type(X, 25-X) :- atom(X).
% ^ FIXME: move type case to C side?
% we create the dicts there

doit(X,A, R) :-
    current_prolog_flag(xt_connection_info, ConnInfo),
    setup_call_cleanup(swixt_pg_connect(ConnInfo, Conn),
                       (maplist(xt_type, A, TypedArgs),
                        swixt_pg_query(Conn,X,TypedArgs,R)),
                       swixt_pg_close(Conn)).

connect(ConnectionName, Host, Port) :-
    current_prolog_flag(xt_odbc_driver, D),
    atomic_list_concat(
        ['Driver={', D, '};Server=', Host, ';Port=', Port, ';Database=xtdb;Uid=;Pwd='],
        DriverString),
    odbc_connect(ConnectionName, _,
                 [ driver_string(DriverString), alias(ConnectionName) ]).

connect(Host,Port) :-
    current_prolog_flag(xt_connection, Con),
    connect(Con,Host,Port).

connect(Host) :- connect(Host,5432).
connect :- connect('localhost').

odbc_q(Con, SQL, Results) :-
    odbc_query(Con, SQL, Results, [source(true)]).

odbc_q(SQL, Results) :-
    current_prolog_flag(xt_connection, Con),
    odbc_q(Con, SQL, Results).

q(Sql, Parameters, Results) :-
    maplist(nth0(0), Parameters, ArgTypes),
    maplist(nth0(1), Parameters, ArgVals),
    odbc_prepare(mydb, Sql, ArgTypes, Qid),
    odbc_execute(Qid, ArgVals, Results),
    odbc_free_statement(Qid).


xt_post(Path, Json, Results) :-
    current_prolog_flag(xt_url, BaseUrl),
    format(atom(Url), '~w/~w', [BaseUrl,Path]),
    http_open(Url, Stream, [post(json(Json))]),
    json_read_dict(Stream, Results, [tag('@type'),default_tag(data)]).

query(Sql, Args, Results) :-
    to_json(Args, JsonArgs),
    xt_post(query, d{sql: Sql, queryOpts: d{args: JsonArgs}}, Results0),
    once(json_prolog(Results0, Results)).

to_arg_ref(N,Ref) :- format(atom(Ref), '$~d', [N]).

insert_into(Table, Fields, ValueRows, tx{sql: SQL, argRows: ValueRows}) :-
    atomic_list_concat(Fields, ',', FieldList),
    length(Fields, NumArgs),
    numlist(1, NumArgs, Args),
    maplist(to_arg_ref, Args, ArgRefs),
    atomic_list_concat(ArgRefs, ',', ArgNums),
    format(string(SQL), 'INSERT INTO ~w (~w) VALUES (~w)', [Table, FieldList, ArgNums]).

insert(Dict, TxOp) :-
    dict_pairs(Dict, Table, Fields),
    pairs_keys(Fields, Ks),
    pairs_values(Fields, Vs),
    insert_into(Table, Ks, [Vs], TxOp).

status(Status) :-
    current_prolog_flag(xt_url, BaseUrl),
    format(atom(Url), '~w/status', [BaseUrl]),
    http_open(Url, Stream, []),
    json_read_dict(Stream, Status, [tag('@type'),default_tag(xt)]).

tx(TxOpCalls, tx{systemTime: SystemTime, id: TxId}) :-
    maplist([TxOpCall,TxOp]>>(call(TxOpCall, TxOp)), TxOpCalls, TxOps),
    once(json_prolog(TxOpsJson, TxOps)),
    xt_post(tx, tx{txOps: TxOpsJson}, Result),
    _{'systemTime': SystemTimeStr, 'txId': TxId} :< Result,
    string_datetimetz(SystemTimeStr, SystemTime).

%%% State
% The state of building a SQL query consists of a dict that contains the
% different parts (projection, table, alias, where clauses, order, etc).
% Parts like clauses are lists that are prepended to and reversed
% when finally processed into a full SQL clause and arguments.

new_state(Table, q{alias: [a],
                   next_alias: b,
                   args: 1-[],
                   table: Table,
                   projection: ['*'],
                   where: [],
                   order: '',
                   % Only used for subqueries (NEST_MANY or NEST_ONE)
                   cardinality: 'MANY',
                   bind: unsupported % list of bindings, only supported in q/1
                  }).

new_state_with(Table, Fields, State) :-
    new_state(Table, S0),
    put_dict(Fields, S0, State).

new_state_with(Table, OldState, KeepFields, State) :-
    new_state(Table, S0),
    foldl({OldState}/[Keep, SIn, SOut]>>( get_dict(Keep, OldState, Val),
                                          put_dict([Keep=Val], SIn,SOut) ),
          KeepFields, S0, State).

state(S), [S] --> [S].
state(S0, S1), [S1] --> [S0].

push_state(NewState), [NewState, S] --> [S].
pop_state(Popped), [S] --> [Popped, S].

debug(Term) :- current_prolog_flag(xt_debug, D), debug(D, Term).
debug(false, _).
debug(true, T) :- writeln(T).

%% Add new argument, Ref unifies with the number
arg(Val, Ref) -->
    state(S0, S1),
    { get_dict(args, S0, Cur-Args),
      succ(Cur, Next),
      to_arg_ref(Cur, Ref),
      put_dict([args=Next-[Val|Args]], S0, S1)
    }.

%% Add formatted where fragment
where_(Fmt, Args) -->
    state(S0, S1),
    { get_dict(where, S0, Where),
      format(atom(SQL), Fmt, Args),
      put_dict([where=[SQL|Where]], S0, S1) }.

%% Add formatted select fragment
select_(Fmt, Args) -->
    state(S0, S1),
    { get_dict(projection, S0, Proj),
      format(atom(SQL), Fmt, Args),
      put_dict([projection=[SQL|Proj]], S0, S1) }.

remove_star -->
    state(S0, S1),
    { get_dict(projection, S0, Proj),
      exclude(=('*'), Proj, WithoutStar),
      put_dict([projection=WithoutStar], S0, S1) }.

pushalias -->
    state(S0, S1),
    { _{alias: Alias, next_alias: NewAlias} :< S0,
      char_code(NewAlias, Code),
      succ(Code, Code1),
      char_code(NextAlias, Code1),
      put_dict([alias=[NewAlias|Alias], next_alias=NextAlias], S0, S1) }.

popalias -->
    state(S0, S1),
    { _{alias: [_|Alias]} :< S0,
      put_dict([alias=Alias], S0, S1) }.

alias(A) -->
    state(S),
    { _{alias: [A|_]} :< S }.


%%%%%%%%
%% Format state into SQL clause with arguments

to_sql(State, SQL, Args) :-
    debug(state(State)),
    _{alias: [Alias|_], table: Table, projection: ProjRev, where: Where,
      args: _-ArgsRev, order: OrderBy} :< State,
    reverse(ArgsRev, Args),
    reverse(ProjRev, Proj),
    atomic_list_concat(Proj, ', ', Projection),
    combined_where_clause(Where, WhereClause),
    format(string(SQL), 'SELECT ~w, ''~w'' as "@type" FROM ~w ~w ~w ~w',
           [Projection, Table, Table, Alias, WhereClause, OrderBy]).

to_sql_delete(State, SQL, Args) :-
    _{alias: [Alias|_], table: Table, where: Where,
      args: _-ArgsRev} :< State,
    reverse(ArgsRev, Args),
    combined_where_clause(Where, WhereClause),
    format(string(SQL), 'DELETE FROM ~w ~w ~w', [Table, Alias, WhereClause]).


combined_where_clause([], '').
combined_where_clause(Where, SQL) :-
    length(Where, L), L > 0,
    atomic_list_concat(Where, ' AND ', CombinedWhere),
    format(atom(SQL), ' WHERE ~w', [CombinedWhere]).



%% Handle a Field-Val pair in dict
% it might be a where clause, a nested query or a special

special_field('_only').
special_field('_order').
special_field('_cardinality').

handle([Fv|Fvs]) -->
    handle(Fv),
    handle(Fvs).

handle([]) --> [].

handle(Field-Var) -->
    { ground(Field), var(Var) },
    aliased_field(Field, Field1),
    state(S0, S1),
    { _{bind: Bindings, projection: Projection} :< S0,
      (Bindings = unsupported -> throw(bindings_unsupported("use q/1")); true),
      put_dict([bind=[Field-Var|Bindings], projection=[Field1|Projection]], S0, S1) }.

handle(Field-Val) -->
    % Don't handle special fields or nested dictionaries
    { \+ special_field(Field), \+ is_dict(Val), \+ var(Val) },
    where(Field, Val).

handle('_only'-Lst) -->
    remove_star,
    state(S0, S1),
    { put_dict([projection=Lst], S0, S1) }.

handle('_order'-By) -->
    state(S0, S1),
    { order_field_dir(By, Field, Dir),
      format(atom(SQL), ' ORDER BY ~w ~w', [Field, Dir]),
      put_dict([order=SQL], S0, S1) }.

handle('_cardinality'-one) -->
    state(S0, S1),
    { put_dict([cardinality='ONE'], S0, S1) }.
handle('_cardinality'-many) -->
    state(S0, S1),
    { put_dict([cardinality='MANY'], S0, S1) }.

handle(Field-Dict) -->
    { is_dict(Dict), dict_pairs(Dict, Table, Pairs) },
    pushalias,
    state(S0),
    { new_state_with(Table, S0, [alias, next_alias, args], State) },
    push_state(State),
    handle(Pairs),
    pop_state(SubQuery),
    % Restore alias and args from subquery state back to main state
    state(State1, State2),
    { _{next_alias: NextAlias, args: ArgsC} :< SubQuery,
      put_dict([next_alias=NextAlias, args=ArgsC], State1, State2) },
    popalias,
    { to_sql(SubQuery, SQL, _Args),
      _{cardinality: Cardinality} :< SubQuery },
    select_('NEST_~w(~w) AS ~w', [Cardinality, SQL, Field]).

% Combine where clause and a bind variable
handle(Field-(Where/Var)) -->
    { ground(Where), var(Var) },
    handle(Field-Var),
    handle(Field-Where).

order_field_dir(Field, Field, 'ASC') :- atom(Field).
order_field_dir(Field-asc, Field, 'ASC').
order_field_dir(Field-desc, Field, 'DESC').

aliased_field(Field, Aliased) -->
    alias(A),
    { format(atom(Aliased), '~w.~w', [A, Field]) }.

where(Field, Val) -->
    %% Not a compound value, this is a direct equality
    { \+ compound(Val) },
    aliased_field(Field, Aliased),
    where(Aliased, '=', [Val]).

where(Field, Val) -->
    { compound(Val), compound_name_arguments(Val, Op, Args) },
    aliased_field(Field, Aliased),
    where(Aliased, Op, Args).

where(Field, '=', [Val]) --> arg(Val, Ref), where_('~w = ~w', [Field, Ref]).
where(Field, '<', [Val]) --> arg(Val, Ref), where_('~w < ~w', [Field, Ref]).
where(Field, '>', [Val]) --> arg(Val, Ref), where_('~w > ~w', [Field, Ref]).
where(Field, '<=', [Val]) --> arg(Val, Ref), where_('~w <= ~w', [Field, Ref]).
where(Field, '>=', [Val]) --> arg(Val, Ref), where_('~w >= ~w', [Field, Ref]).
where(Field, like, [Val]) --> arg(Val, Ref), where_('~w LIKE ~w', [Field, Ref]).
where(Field, between, [Min,Max]) -->
    arg(Min, MinRef), arg(Max, MaxRef), where_('(~w BETWEEN ~w AND ~w)', [Field, MinRef, MaxRef]).
where(Field, (^), [ParentField]) -->
    state(S0),
    { get_dict(alias, S0, [_, ParentAlias | _]) },
    where_('~w = ~w.~w', [Field, ParentAlias, ParentField]).

q(Candidate, Results) :-
    dict_pairs(Candidate, Table, Pairs),
    new_state(Table, S0),
    phrase(handle(Pairs), [S0], [S1]),
    to_sql(S1, SQL, Args),
    debug('FINAL_SQL'(SQL)),
    query(SQL, Args, Results).

% Logical version of query, each row will bind any variables
% in the candidate fields.
q(Candidate) :-
    dict_pairs(Candidate, Table, Pairs),
    new_state_with(Table, [projection=[], bind=[]], S0),
    phrase(handle(Pairs), [S0], [S1]),
    to_sql(S1, SQL, Args),
    debug('FINAL_SQL'(SQL)),
    query(SQL, Args, Results),
    member(Row, Results),
    _{bind: Bindings} :< S1,
    maplist({Row}/[Field-Var]>>get_dict(Field, Row, Var), Bindings).

%% PENDING: how about JOINs (apart from NEST_MANY/_ONE use)
% we could use a logic variable to determine the join condition
% example:
% products{'_id': PId, price: P} ^ orderline{product_id: PId, quantity: Q}
%
% this get orderline quantity and product price joined by product id
% what about time?

delete(Candidate, tx{sql: SQL, argRows: [Args]}) :-
    dict_pairs(Candidate, Table, Pairs),
    new_state(Table, S0),
    phrase(handle(Pairs), [S0], [S1]),
    to_sql_delete(S1, SQL, Args),
    debug('FINAL_DELETE_SQL'(SQL)).

update(Candidate, Fields, TxOp) :-
    throw(not_implemented_yet("Use raw to do update for now")).
    %% Candidate determines the where clause
    %% and fields is expression to do an update, fixme: what should be supported?
    %%
    %% at least setting to direct values, like: {value: 42}
    %% what about arithmetic expressions? {value: value * 1.10}

%%
% aggregates sub query
% q(products{name: Name,
%            price: Price,
%            bought: sum(Bought, quantity, orderline{product_id: ^('_id')})}.


%% Raw SQL clause to run.
raw(SQL, tx{sql: SQL, argRows: [[]]}).
raw(SQL, ArgRows, tx{sql: SQL, argRows: ArgRows}).

%%%%%%%%%%
% Test suite

:- begin_tests(swixt, [setup(init_test_data)]).

init_test_data :-
    tx([ insert(person{'_id': 1, name: "Max Syöttöpaine"}),
         insert(person{'_id': 2, name: "Barbara Jenkins"}),

         insert(todo{'_id': 1, item: "make some test data", done: true, assignee: 1}),
         insert(todo{'_id': 2, item: "implement more features", done: false, assignee: 1}),
         insert(todo{'_id': 3, item: "update readme", done: true}),
         insert(todo{'_id': 4, item: "implement operators", done: true}),
         insert(todo{'_id': 5, item: "write tests", done: true, assignee: 1}),
         insert(todo{'_id': 6, item: "gain mass popularity", done: false, assignee: 2})
       ], _).

test(basic_query_with_id) :-
    q(todo{'_id': 1}, [todo{'_id': 1, item: "make some test data", done: true, assignee: 1}]).

test(select_only_some_fields) :-
    q(todo{'_only': ['_id',done],'_order':'_id'-asc},
      [todo{'_id':1,done:true},
       todo{'_id':2,done:false},
       todo{'_id':3,done:true},
       todo{'_id':4,done:true},
       todo{'_id':5,done:true},
       todo{'_id':6,done:false}]).

test(ordering) :-
    tx([ raw("INSERT INTO num (_id, n) VALUES ($1, $2)", [[1, 666], [2, -1234], [3, 420], [4, 13]]) ], _),
    q(num{'_only': [n], '_order': n}, [ num{n: -1234}, num{n: 13}, num{n: 420}, num{n: 666} ]),
    q(num{'_only': [n], '_order': n-asc}, [ num{n: -1234}, num{n: 13}, num{n: 420}, num{n: 666} ]),
    q(num{'_only': [n], '_order': n-desc}, [ num{n: 666}, num{n: 420},  num{n: 13}, num{n: -1234} ]),
    q(num{'_only': [n], '_order': '_id'}, [ num{n: 666}, num{n: -1234}, num{n: 420}, num{n: 13} ]).

ex(todo{'_id': <(2)}, [todo{'_id': 1, item: "make some test data", done: true, assignee: 1}]).
ex(todo{'_only': ['_id'], item: like("%test%"), '_order': '_id'}, [todo{'_id': 1}, todo{'_id': 5}]).
ex(todo{'_id': between(2,4), '_only': ['_id'], '_order': '_id'}, [todo{'_id': 2}, todo{'_id': 3}, todo{'_id': 4}]).

% Find person and nest all their todos
ex(person{'_id': 1, todos: todo{'assignee': ^('_id'), '_only':[item], '_order': '_id'}},
   [person{'_id': 1, name: "Max Syöttöpaine",
           todos: [ todo{item: "make some test data"},
                    todo{item: "implement more features"},
                    todo{item: "write tests"} ]}]).

ex(person{'_id': 1, todos: todo{'assignee': ^('_id'), '_only':[item], done: false}},
   [person{'_id': 1, name: "Max Syöttöpaine",
           todos: [ todo{item: "implement more features"} ]}]).

% Find incomplete todos and nest the one assignee
ex(todo{done: false,
        assigned: person{'_id': ^(assignee), '_cardinality': one},
        '_order': '_id'},
   [ todo{'_id':2, assigned:person{'_id':1, name:"Max Syöttöpaine"}, assignee:1, done:false, item:"implement more features"},
     todo{'_id':6, assigned:person{'_id':2, name:"Barbara Jenkins"}, assignee:2, done:false, item:"gain mass popularity"}]).


test(queries, [forall(ex(Candidate,Results))]) :- q(Candidate,Results).

:- end_tests(swixt).
