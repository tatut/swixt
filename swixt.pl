:- module(swixt, [q/2, q/1, insert/1, insert/2, status/1]).
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

query(Sql, Results) :-
    xt_post(query, d{sql: Sql}, Results).

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

where([], ''). % no fields, don't emit a WHERE clause
where(Fields, FormattedWhereClause) :-
    length(Fields, Len), Len > 0,
    maplist(format_where, Fields, FieldClauses),
    atomic_list_concat(FieldClauses, ' AND ', FieldClausesJoined),
    format(atom(FormattedWhereClause), ' WHERE ~w', [FieldClausesJoined]).

special_field('_only').
special_field('_order').
special_field_pair(Field-_) :- special_field(Field).

%% Partition fields into regular and special control fields
partition_fields(FieldsIn, ClauseFields, SpecialFields) :-
    partition(special_field_pair, FieldsIn, SpecialFields, ClauseFields).

format_where(Field- >(V), Out) :-
    format(atom(Out), '~w > ~w', [Field, V]).
format_where(Field- <(V), Out) :-
    format(atom(Out), '~w < ~w', [Field, V]).
format_where(Field-between(Min,Max), Out) :-
    format(atom(Out), '(~w BETWEEN ~w AND ~w)', [Field, Min, Max]).
format_where(Field-like(Str), Out) :-
    %%% FIXME: have query PARAMETERS instead of strings mashed together
    format(atom(Out), '~w LIKE ''~w''', [Field,Str]).
format_where(Field-not(Clause), Out) :-
    format_where(Field-Clause, Where),
    format(atom(Out), 'NOT (~w)', [Where]).
format_where(Field-Val, Out) :-
    \+ compound(Val),
    format(atom(Out), '~w = ~w', [Field, Val]).

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

dict_sql(Dict, SQL) :-
    dict_pairs(Dict, Table, Fields),
    partition_fields(Fields, ClauseFields, SpecialFields),
    where(ClauseFields, FormattedWhereClause),
    order_by(SpecialFields, OrderByClause),
    projection(SpecialFields, Projection),
    format(atom(SQL),
           'SELECT ~w FROM ~w~w~w',
           [Projection, Table, FormattedWhereClause, OrderByClause]).

%%%%%%
%% The main query interface

q(Candidate, Results) :-
    dict_sql(Candidate, SQL),
    writeln(query(SQL)),
    query(SQL, Results).

insert(Dict, Result) :-
    dict_pairs(Dict, Table, Fields),
    pairs_keys(Fields, Ks),
    pairs_values(Fields, Vs),
    insert_into(Table, Ks, [Vs], Result).

insert(Dict) :-
    insert(Dict, _).
