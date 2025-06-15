/* Parse JSON-LD received over the wire directly to
 * Prolog terms.
 *
 * Handles special @type definitions and turns them into
 * compound terms or dicts with that tag.
 *
 * If object is like {"@type": "xt:timestamp", "@value": "2025-06-..."}
 * it is turned into compound term timestamp(2025,6,...).
 *
 * If the "@type" is not a predefined data type, it is presumed to
 * denote a table and the object is turned into a dict with that tag.
 * So {"@type":"customer", "name": "Foo"} becomes customer{name="Foo"}
 *
 * Parsing does not modify the JSON input.
 */
#include "SWI-Prolog.h"
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include "json.h"

#define expect(expr)                                                           \
  if (!(expr))                                                          \
  return false


void skipws(char **at) {
  //printf("skipping, at: %c\n", **at);
  char c = **at;
  while(c == ' ' || c == '\t' || c == '\n' || c == '\r') {
    //printf("char is %c\n", c);
    *at = *at + 1;
    c = **at;
  }
}

bool is_alpha(char ch) {
  return (ch >= 'a' && ch <= 'z') ||
    (ch >= 'A' && ch <= 'Z');
}

bool is_digit(char ch) { return ch >= '0' && ch <= '9'; }

bool is_alphanumeric(char ch) { return is_alpha(ch) || is_digit(ch); }

static bool looking_at(char *at, char *word) {
  size_t c = 0;
  while(*word != 0) {
    if(*at == 0) return false;
    if(*at != *word) return false;
    at++;
    word++;
    c++;
  }
  return true;
}
static bool looking_at_then(char *at, char *word, char **next) {
  if(looking_at(at, word)) {
    *next = at + strlen(word);
    return true;
  }
  return false;
}


static bool parse_number(char *at, term_t to, char **after) {
  long f = 1;
  if(*at == '-') {
    f = -1;
    at++;
  }
  long num = 0;
  while(is_digit(*at)) {
    num *= 10;
    num += *at - '0';
    at++;
  }
  *after = at;
  if(*at == '.') {
    at++;
    // fraction
    double fr = 1;
    double frac = 0;
    while(is_digit(*at)) {
      fr *= 10;
      frac = 10*frac + (*at - '0');
      at++;
    }
    *after = at;
    double v = (double) f * ((double) num + frac/fr);
    return PL_unify_float(to, v);
  } else {
    return PL_unify_uint64(to, f*num);
  }
}

// read simple '"' delimited string of maxlen
static bool read_str(char *at, size_t len, char *to, char **after) {
  char *end = at+1;
  while(*end != '"') end++;
  *after = end+1;
  if(end-at > len) return false;
  strncpy(to, at+1, end-at-1);
  to[end-at-1] = 0;
  return true;
}
static bool parse_string(char *at, term_t to, char **after) {
  // Naive first attempt, just take bytes until '"'
  // FIXME: support all escapes in JSON strings!
  char *end = at+1;
  while(*end != '"') end++;
  *after = end+1;
  return PL_unify_string_nchars(to, end-at-1, at+1);
}

static bool read_int(char *at, long *num, char **after) {
  if(!is_digit(*at)) return false;
  *num = 0;
  while(is_digit(*at)) {
    *num *= 10;
    *num += *at - '0';
    at++;
  }
  *after = at;
  return true;
}


static bool parse_timestamp(term_t to, char *at) {
  long year, month, day, hour, minute, seconds=0, micros=0;
  // 2025-06-14T17:45:12.666420 (seconds and micros optional)
  expect(read_int(at, &year, &at));
  expect(*at == '-'); at++;
  expect(read_int(at, &month, &at));
  expect(*at == '-'); at++;
  expect(read_int(at, &day, &at));
  expect(*at == 'T'); at++;
  expect(read_int(at, &hour, &at));
  expect(*at == ':'); at++;
  expect(read_int(at, &minute, &at));
  if(*at == 0) goto end;
  expect(*at == ':'); at++;
  expect(read_int(at, &seconds, &at));
  if(*at == '"') goto end;
  expect(*at == '.'); at++;
  expect(read_int(at, &micros, &at));
 end:
  // construct the term and read the ending '"'
  expect(*at == 0);
  return PL_unify_term(to,
                       PL_FUNCTOR_CHARS, "timestamp", 7,
                       PL_LONG, year,
                       PL_LONG, month,
                       PL_LONG, day,
                       PL_LONG, hour,
                       PL_LONG, minute,
                       PL_LONG, seconds,
                       PL_LONG, micros);
}

static bool parse_special(term_t to, char *type, char *value) {
  if(strcmp(type, "xt:timestamp")==0) {
    return parse_timestamp(to, value);
  } else if(strcmp(type, "xt:date")==0) {
    // FIXME;
    return false;
  } else {
    fprintf(stderr, "Unrecognized special @type: %s\n", type);
    return false;
  }
}

#define MAX_KEY_LEN 128
#define MAX_OBJECT 256
#define MAX_VALUE_LEN 256

static bool parse_object(char *at, term_t to, char **after) {
  expect(*at == '{'); at++;
  char type[MAX_KEY_LEN];
  char value[MAX_VALUE_LEN];
  bool has_type = false, has_value = false;
  atom_t tag = 0;

  // PENDING: we could have a dynarray, but this should be plenty
  atom_t keys[MAX_OBJECT];
  term_t vals[MAX_OBJECT];
  size_t k=0;
  skipws(&at);
  if(*at == '}') { at++;  goto done; } // empty object (apart from possible tag)
  while(true) {
    if(k == MAX_OBJECT) {
      fprintf(stderr, "Too many object values, can't have more than %d\n", MAX_OBJECT);
      return false;
    }
    term_t key, val;

    expect(*at == '"'); // keys must be strings
    char keyname[MAX_KEY_LEN];
    if(!read_str(at, 128, keyname, &at)) return false;
    skipws(&at);
    expect(*at == ':'); at++; // must have ':' between key and value
    skipws(&at);

    if(keyname[0] == '@') {
      if(strcmp(keyname, "@type")==0) {
        // this is a tag for the object or a special type
        if(!read_str(at, MAX_KEY_LEN, type, &at)) return false;
        has_type = true;
        if(has_value) {
          skipws(&at);
          expect(*at == '}'); at++;
          *after = at;
          return parse_special(to, type, value);
        }
        goto next;
      } else if(strcmp(keyname, "@value")==0) {
        if(!read_str(at, MAX_VALUE_LEN, value, &at)) return false;
        has_value = true;
        if(has_type) {
          skipws(&at);
          expect(*at == '}'); at++;
          *after = at;
          return parse_special(to, type, value);
        }
        goto next;
      }
    }

    key = PL_new_atom(keyname);
    val = PL_new_term_ref();
    if(!json_parse(at, val, &at)) return false;
    keys[k] = key;
    vals[k] = val;
    k++;

  next:

    skipws(&at);
    if(*at == ',') {
      at++;
      skipws(&at);
    } else {
      expect(*at == '}');
      at++;
      goto done;
    }
  }

 done:
  *after = at;
  // construct the dict
  term_t valterms = PL_new_term_refs(k);
  for(size_t i=0; i<k; i++) {
    if(!PL_unify((valterms+i),vals[i])) return false;
  }
  term_t dict = PL_new_term_ref();
  if(has_type) tag = PL_new_atom(type);
  if(!PL_put_dict(dict, tag, k, keys, valterms)) return false;
  return PL_unify_term(to, PL_TERM, dict);

}

typedef struct ListParse {
  term_t item;
  bool success;
} ListParse;

// list is read by recursing and creating the cons cells
// when unwinding, so we get the list in proper order
static ListParse parse_list_(bool first, char *at, char **after) {
  skipws(&at);
  if(*at == ']') {
    // at end of list, set after and return nil ref
    *after = at + 1;
    return (ListParse) { PL_new_nil_ref(), true };
  } else {
    // parse this item, and recursively parse next
    if(!first) {
      if(*at != ',') goto fail;
      at++;
    }
    skipws(&at);
    term_t item = PL_new_term_ref();
    if(!json_parse(at, item, &at)) goto fail;
    ListParse rest = parse_list_(false, at, after);
    if(rest.success) {
      term_t res = PL_new_term_ref();
      if(!PL_cons_list(res, item, rest.item)) goto fail;
      return (ListParse) { res, true };
    }
  }
 fail:
  return (ListParse) { PL_new_nil_ref(), false };
 }

static bool parse_list(char *at, term_t to, char **after) {
  ListParse l = parse_list_(true, at+1, after);
  if(l.success) {
    return PL_unify(l.item, to);
  } else {
    return false;
  }
}

bool json_parse(char *at, term_t to, char **after) {
  char ch = *at;
  switch(*at) {
  case '-':
  case '0': case '1': case '2': case '3': case'4':
  case '5': case '6': case '7': case '8': case '9':
    return parse_number(at, to, after);

  case '"': return parse_string(at, to, after);
  case '{': return parse_object(at, to, after);
  case '[': return parse_list(at, to, after);
  default:
    if(looking_at(at, "null") && !is_alphanumeric(at[4])) {
      *after = at + 4;
      return PL_unify_atom(to, PL_new_atom("nil"));
    }
    if(looking_at(at, "true") && !is_alphanumeric(at[4])) {
      *after = at + 4;
      return PL_unify_atom(to, PL_new_atom("true"));
    }
    if(looking_at(at, "false") && !is_alphanumeric(at[5])) {
      *after = at + 5;
      return PL_unify_atom(to, PL_new_atom("false"));
    }
    return false;
  }
}

bool json_parse_toplevel(char *at, term_t to) {
  char *after;
  if(!json_parse(at, to, &after)) return false;
  skipws(&after);
  return *after == 0;
}
