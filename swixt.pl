:- module(swixt, [q/2, q/1, insert/1, insert/2, status/1]).
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

q(Candidate, Results) :-
    dict_pairs(Candidate, Table, Fields),
    where(Fields, FormattedWhereClause),
    format(atom(SQL), 'SELECT * FROM ~w~w', [Table, FormattedWhereClause]),
    writeln(query(SQL)),
    query(SQL, Results).

insert(Dict, Result) :-
    dict_pairs(Dict, Table, Fields),
    pairs_keys(Fields, Ks),
    pairs_values(Fields, Vs),
    insert_into(Table, Ks, [Vs], Result).

insert(Dict) :-
    insert(Dict, _).
