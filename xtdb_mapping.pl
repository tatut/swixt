%% Mapping to/from XTDB JSON LD types
:- module(xtdb_mapping, [json_prolog/2, to_json/2, string_datetimetz/2]).
:- use_module(library(dcg/basics)).
:- use_module(library(yall)).
:- set_prolog_flag(double_quotes, codes).

%% XT type mapping, see: https://github.com/xtdb/xtdb/blob/main/api/src/main/kotlin/xtdb/JsonSerde.kt#L60
xt_type('xt:instant').
xt_type('xt:timestamptz').
xt_type('xt:timestamp').
xt_type('xt:date').
xt_type('xt:duration').
xt_type('xt:timeZone').
xt_type('xt:period').
xt_type('xt:keyword').
xt_type('xt:symbol').
xt_type('xt:uuid').
xt_type('xt:set').

% Types passed through unchanged: numbers, strings, booleans, null
json_prolog(V, V) :- string(V).
json_prolog(V, V) :- number(V).
json_prolog(true, true).
json_prolog(false, false).
json_prolog(null, null).
json_prolog([], []).

json_prolog([JsonValue|JsonValues], [PrologValue|PrologValues]) :-
    json_prolog(JsonValue, PrologValue),
    json_prolog(JsonValues, PrologValues).


% Any dict whose tag is not an XT type, is a table result,
% recursively process all the fields.
json_prolog(D0, D1) :-
    (ground(D0)
    -> (is_dict(D0, Tag), \+ xt_type(Tag), dict_pairs(D0, Tag, Pairs0))
    ; (is_dict(D1), dict_pairs(D1, Tag, Pairs1))),
    maplist([K-V0,K-V1]>>json_prolog(V0,V1), Pairs0, Pairs1),
    dict_pairs(D0, Tag, Pairs0),
    dict_pairs(D1, Tag, Pairs1).

json_prolog('xt:timestamp'{'@value': Instant}, datetime(Year,Month,Date,Hour,Minute,Second,Millisecond)) :-
    (ground(Instant) -> string_codes(Instant, Codes); true),
    phrase(datetime(Year, Month, Date, Hour, Minute, Second, Millisecond), Codes, []),
    string_codes(Instant, Codes).

json_prolog('xt:set'{'@value': JsonList}, set(PrologList)) :-
    (ground(JsonList) -> is_list(JsonList); is_list(PrologList)),
    json_prolog(JsonList, PrologList).

json_prolog('xt:date'{'@value': DateStr}, date(Year,Month,Date)) :-
    (ground(DateStr) -> string_codes(DateStr, Codes); true),
    phrase(date(Year,Month,Date), Codes, []),
    string_codes(DateStr, Codes).

pad0(L, In, In) :- length(In, L). % no more padding needed
pad0(L, In, Out) :-
    length(In, L0), L0 < L,
    append("0", In, Intermediate),
    pad0(L, Intermediate, Out).

int(L, Number) --> { (ground(Number) -> number_codes(Number, Cs0), pad0(L, Cs0, Cs); true) },
                   digits(Cs),
                   { number_codes(Number, Cs) }.

string_datetimetz(String, datetimetz(Y,M,D,H,Mi,S,Ns,Tz)) :- string_codes(String, Cs), phrase(datetimetz(Y,M,D,H,Mi,S,Ns,Tz), Cs, []).

% "2024-08-14T16:40:55.666000"
datetime(Year,Month,Date,Hour,Minute,Second,Nanos) -->
    date(Year,Month,Date), "T", time(Hour,Minute,Second,Nanos).

datetimetz(Year,Month,Date,Hour,Minute,Second,Nanos,Tz) -->
    date(Year,Month,Date), "T", time(Hour,Minute,Second,Nanos), tz(Tz).

tz(0) --> "Z". %% FIXME: support tz like "+03:00" or "[Europe/Helsinki]"?
date(Year,Month,Date) --> int(4,Year), "-", int(2,Month), "-", int(2,Date).
time(Hour,Minute,Second,Nanos) --> int(2,Hour), ":", int(2,Minute), ":", int(2,Second), ".", integer(Nanos).

dict_json_ld(V, V) :- \+ is_dict(V), \+ is_list(V).
dict_json_ld([],[]).
dict_json_ld([J|Js], [L|Ls]) :- dict_json_ld(J, L), dict_json_ld(Js, Ls).
dict_json_ld(Tag{'@value': Value}, data{'@type': Tag, '@value': Value}).

to_json(Prolog, Json) :-
    json_prolog(Json0, Prolog),
    dict_json_ld(Json0, Json).
