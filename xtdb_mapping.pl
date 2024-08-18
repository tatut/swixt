%% Mapping to/from XTDB JSON LD types
:- module(xtdb_mapping, [json_prolog/2, to_json/2]).
:- use_module(library(dcg/basics)).
:- set_prolog_flag(double_quotes, codes).

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

% "2024-08-14T16:40:55.666"
datetime(Year,Month,Date,Hour,Minute,Second,Millisecond) -->
    date(Year,Month,Date), "T", time(Hour,Minute,Second,Millisecond).

date(Year,Month,Date) --> int(4,Year), "-", int(2,Month), "-", int(2,Date).
time(Hour,Minute,Second,Millisecond) --> int(2,Hour), ":", int(2,Minute), ":", int(2,Second), ".", int(3,Millisecond).

dict_json_ld(V, V) :- \+ is_dict(V), \+ is_list(V).
dict_json_ld([],[]).
dict_json_ld([J|Js], [L|Ls]) :- dict_json_ld(J, L), dict_json_ld(Js, Ls).
dict_json_ld(Tag{'@value': Value}, data{'@type': Tag, '@value': Value}).

to_json(Prolog, Json) :-
    json_prolog(Json0, Prolog),
    dict_json_ld(Json0, Json).
