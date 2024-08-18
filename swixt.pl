:- module(swixt, [q/2, q/1, insert/1, insert/2, status/1]).
:- use_module(xtdb_mapping, [json_prolog/2, to_json/2]).
:- use_module(library(yall)).
:- use_module(library(apply)).
:- use_module(library(http/http_open)).
:- use_module(library(http/http_client)).
:- use_module(library(http/json)).
:- use_module(library(http/http_json)).
:- set_prolog_flag(xt_url, 'http://localhost:6543').

xt_post(Path, Json, Results) :-
    current_prolog_flag(xt_url, BaseUrl),
    format(atom(Url), '~w/~w', [BaseUrl,Path]),
    http_open(Url, Stream, [post(json(Json))]),
    json_read_dict(Stream, Results, [tag('@type'),default_tag(data)]).

query(Sql, Args, Results) :-
    to_json(Args, JsonArgs),
    writeln(query_json_args(JsonArgs)),
    xt_post(query, d{sql: Sql, queryOpts: d{args: JsonArgs}}, Results0),
    json_prolog(Results0, Results).

to_arg_ref(N,Ref) :- format(atom(Ref), '$~d', [N]).

insert_into(Table, Fields, ValueRows, tx{txOps: [tx{sql: SQL, argRows: ValueRows}]}) :-
    atomic_list_concat(Fields, ',', FieldList),
    length(Fields, NumArgs),
    numlist(1, NumArgs, Args),
    maplist(to_arg_ref, Args, ArgRefs),
    atomic_list_concat(ArgRefs, ',', ArgNums),
    format(atom(SQL), 'INSERT INTO ~w (~w) VALUES (~w)', [Table, FieldList, ArgNums]),
    writeln(insert(SQL)).

insert(Dict) :-
    insert(Dict, TxOp),
    xt_post(tx, tx{txOps: [TxOp]}, _Results).

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
                   cardinality: 'MANY'}).

new_state_with(Table, OldState, KeepFields, State) :-
    new_state(Table, S0),
    foldl({OldState}/[Keep, SIn, SOut]>>( get_dict(Keep, OldState, Val),
                                          put_dict([Keep=Val], SIn,SOut) ),
          KeepFields, S0, State).

state(S), [S] --> [S].
state(S0, S1), [S1] --> [S0].

push_state(NewState), [NewState, S] --> [S].
pop_state(Popped), [S] --> [Popped, S].

debug -->
    state(S),
    { writeln(current_state(S)) }.

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
    writeln(state(State)),
    _{alias: [Alias|_], table: Table, projection: ProjRev, where: Where,
      args: _-ArgsRev} :< State,
    reverse(ArgsRev, Args),
    reverse(ProjRev, Proj),
    writeln(args(Args)),
    atomic_list_concat(Proj, ', ', Projection),
    combined_where_clause(Where, WhereClause),
    format(atom(SQL), 'SELECT ~w, ''~w'' as "@type" FROM ~w ~w ~w', [Projection, Table, Table, Alias, WhereClause]).

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

handle(Field-Val) -->
    % Don't handle special fields or nested dictionaries
    { \+ special_field(Field), \+ is_dict(Val) },
    where(Field, Val).

handle('_only'-Lst) -->
    { writeln(handling_only(Lst)) },
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
    { new_state_with(Table, S0, [alias, next_alias, args], State),
      writeln(new_state_is(State))
    },
    push_state(State),
    { writeln(handling_nested_pairs(Pairs)) },
    debug,
    handle(Pairs),
    pop_state(SubQuery),
    % Restore alias and args from subquery state back to main state
    state(State1, State2),
    { _{next_alias: NextAlias, args: ArgsC} :< SubQuery,
      put_dict([next_alias=NextAlias, args=ArgsC], State1, State2) },
    debug,
    popalias,
    { to_sql(SubQuery, SQL, Args),
      _{cardinality: Cardinality} :< SubQuery,
      writeln(subquery(SQL)) },
    select_('NEST_~w(~w) AS ~w', [Cardinality, SQL, Field]),
    debug.



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
    { compound(Val), compound_name_arguments(Val, Op, Args), writeln(doit(op(Op),args(Args))) },
    aliased_field(Field, Aliased),
    where(Aliased, Op, Args),
    { writeln(field(Field,handled(Val))) }.

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
    writeln('FINAL_SQL'(SQL)),
    query(SQL, Args, Results).
