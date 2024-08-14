:- module(swixt, [q/2, q/1, insert/1, insert/2, status/1]).
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
    args_list(Args, ArgsList),
    xt_post(query, d{sql: Sql, queryOpts: d{args: ArgsList}}, Results).

to_arg_ref(N,Ref) :- format(atom(Ref), '$~d', [N]).

insert_into(Table, Fields, ValueRows, Results) :-
    atomic_list_concat(Fields, ',', FieldList),
    length(Fields, NumArgs),
    numlist(1, NumArgs, Args),
    maplist(to_arg_ref, Args, ArgRefs),
    atomic_list_concat(ArgRefs, ',', ArgNums),
    format(atom(SQL), 'INSERT INTO ~w (~w) VALUES (~w)', [Table, FieldList, ArgNums]),
    writeln(insert(SQL)),
    xt_post(tx, tx{txOps: [tx{sql: SQL, argRows: ValueRows}]}, Results).

status(Status) :-
    current_prolog_flag(xt_url, BaseUrl),
    format(atom(Url), '~w/status', [BaseUrl]),
    http_open(Url, Stream, []),
    json_read_dict(Stream, Status, [tag('@type'),default_tag(xt)]).

%%%%%
%% Structure to build up args, compound term keeps track of current
%% arg number and a list of args.
%% args_list turns the argument structure into a list.

empty_args(args(1,[])).
args_list(args(_,Args), ArgsRev) :- reverse(Args, ArgsRev).
arg(ArgVal, ArgRef, args(Cur, Args), args(Next, [ArgVal|Args])) :-
    succ(Cur, Next),
    to_arg_ref(Cur, ArgRef).



%% Partition fields into regular and special control fields
partition_fields(FieldsIn, ClauseFields, SpecialFields) :-
    partition(special_field_pair, FieldsIn, SpecialFields, ClauseFields).

%%%%%%%
%% Where clause generation

where([], '', Args, Args). % no fields, don't emit a WHERE clause
where(Fields, FormattedWhereClause, ArgsIn, ArgsOut) :-
    length(Fields, Len), Len > 0,
    foldl([Field, SQL0^Args0, [Out|SQL0]^Args1]>>format_where(Field, Out, Args0, Args1),
          Fields, []^ArgsIn, SQLs^ArgsOut),
    reverse(SQLs, FieldClauses),
    atomic_list_concat(FieldClauses, ' AND ', FieldClausesJoined),
    format(atom(FormattedWhereClause), ' WHERE ~w', [FieldClausesJoined]).

special_field('_only').
special_field('_order').
special_field_pair(Field-_) :- special_field(Field).



format_where_op(Field, Op, Arg, Out, Args, Args) :-
    atom(Arg),
    % reference another field, not a parameter
    format(atom(Out), '~w ~w ~w', [Field, Op, Arg]).

format_where_op(Field, Op, Arg, Out, ArgsIn, ArgsOut) :-
    \+ atom(Arg),
    arg(Arg, Ref, ArgsIn, ArgsOut),
    format(atom(Out), '~w ~w ~w', [Field, Op, Ref]).

format_where(Field- >(V), Out, ArgsIn, ArgsOut) :-format_where_op(Field, '>', V, Out, ArgsIn, ArgsOut).
format_where(Field- <(V), Out, ArgsIn, ArgsOut) :-format_where_op(Field, '<', V, Out, ArgsIn, ArgsOut).
format_where(Field-Val, Out, ArgsIn, ArgsOut) :-
    \+ compound(Val),
    format_where_op(Field, '=', Val, Out, ArgsIn, ArgsOut).
format_where(Field-between(Min,Max), Out, ArgsIn, ArgsOut) :-
    arg(Min, MinRef, ArgsIn, Args1),
    arg(Max, MaxRef, Args1, ArgsOut),
    format(atom(Out), '(~w BETWEEN ~w AND ~w)', [Field, MinRef, MaxRef]).
format_where(Field-like(Str), Out, ArgsIn, ArgsOut) :- format_where_op(Field, 'LIKE', Str, Out, ArgsIn, ArgsOut).

format_where(Field-not(Clause), Out, ArgsIn, ArgsOut) :-
    format_where(Field-Clause, Where, ArgsIn, ArgsOut),
    format(atom(Out), 'NOT (~w)', [Where]).


%%%%%%%
%% Ordering, create an ORDER BY clause from the special '_order' field

order_by(Specials, '') :- \+ memberchk('_order'-_, Specials). % no order special field found, no order by
order_by(Specials, OrderByClause) :-
    member('_order'-Order, Specials),
    order_by_clause(Order, OrderField, OrderDir),
    format(atom(OrderByClause), ' ORDER BY ~w ~w', [OrderField, OrderDir]).

order_by_clause(F, F, 'ASC') :- atom(F).
order_by_clause(F-asc, F, 'ASC').
order_by_clause(F-desc, F, 'DESC').

%%%%%%%
%% Projection, by default select everything (*). If '_only' is present, then select only
%% the fields in that list

projection(Specials, '*') :- \+ memberchk('_only'-_, Specials). % no _only special found
projection(Specials, Projection) :-
    member('_only'-Fields, Specials),
    atomic_list_concat(Fields, ',', Projection).

%%%%%%%5
%% Dict to SQL conversion

dict_sql(Dict, SQL, Args) :-
    empty_args(InitialArgs),
    dict_sql(Dict, SQL, InitialArgs, Args).

dict_sql(Dict, SQL, ArgsIn, ArgsOut) :-
    dict_pairs(Dict, Table, Fields),
    partition_fields(Fields, ClauseFields, SpecialFields),
    where(ClauseFields, FormattedWhereClause, ArgsIn, ArgsOut),
    order_by(SpecialFields, OrderByClause),
    projection(SpecialFields, Projection),
    format(atom(SQL),
           'SELECT ~w FROM ~w~w~w',
           [Projection, Table, FormattedWhereClause, OrderByClause]).

%%%%%%
%% The main query interface

q(Candidate, Results) :-
    empty_args(Args0),
    dict_sql(Candidate, SQL, Args0, Args),
    writeln(query(SQL,Args)),
    query(SQL, Args, Results).

insert(Dict, Result) :-
    dict_pairs(Dict, Table, Fields),
    pairs_keys(Fields, Ks),
    pairs_values(Fields, Vs),
    insert_into(Table, Ks, [Vs], Result).

insert(Dict) :-
    insert(Dict, _).
