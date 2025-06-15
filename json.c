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
  to[end-at] = 0;
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


static bool parse_timestamp(char *at, term_t to,  char **after) {
  expect(*at == '"'); at++;
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
  if(*at == '"') goto end;
  expect(*at == ':'); at++;
  expect(read_int(at, &seconds, &at));
  if(*at == '"') goto end;
  expect(*at == '.'); at++;
  expect(read_int(at, &micros, &at));
 end:
  // construct the term and read the ending '"'
  expect(*at == '"'); *after = at + 1;
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


static bool parse_object(char *at, term_t to, char **after) {
  expect(*at == '{'); at++;
  char *type;
  atom_t tag = 0;
  if(looking_at_then(at, "\"@type\":", &type)) {
    // begins with a json-ld type annotation
    // check if we have a predefined known type
    char *value;
    if(looking_at_then(type, "\"xt:timestamp\",\"@value\":", &value)) {
      if(parse_timestamp(value, to, &at)) {
        // if at end of object, we succeeded
        if(looking_at(at, "}")) {
          *after = at + 1;
          return true;
        } else {
          return false;
        }
      } else {
        return false;
      }
      // FIXME } else if(looking_at_then(... for other types
    } else {
      // we have a type tag, which isn't a special type to parse
      // set it as our dict tag
      char tag_str[128];
      if(!read_str(type, 128, tag_str, &at)) return false;
      tag = PL_new_atom(tag_str);
      printf("got tag: %s\n", tag_str);
    }
  }
  printf("parsing object keyvals\n");
  // PENDING: we could have a dynarray, but this should be plenty
  #define MAX_KEYS 256
  atom_t keys[MAX_KEYS];
  term_t vals[MAX_KEYS];
  size_t k=0;
  skipws(&at);
  if(*at == '}') { at++;  goto done; } // empty object (apart from possible tag)
  while(true) {
    if(k == MAX_KEYS) {
      fprintf(stderr, "Too many object values, can't have more than %d\n", MAX_KEYS);
      return false;
    }
    term_t key, val;

    printf("at: %c\n", *at);
    expect(*at == '"'); // keys must be strings
    char keyname[128];
    if(!read_str(at, 128, keyname, &at)) return false;
    printf("parsed key: %s\n", keyname);
    key = PL_new_atom(keyname);

    skipws(&at);
    expect(*at == ':'); at++; // must have ':' between key and value
    skipws(&at);
    val = PL_new_term_ref();
    if(!json_parse(at, val, &at)) return false;
    printf("parsed val\n");
    skipws(&at);
    keys[k] = key;
    vals[k] = val;
    k++;
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
  printf("got %zu key/val pairs\n", k);
  term_t valterms = PL_new_term_refs(k);
  for(size_t i=0; i<k; i++) {
    printf("unify val %zu\n", i);
    if(!PL_unify((valterms+i),vals[i])) return false;
  }
  printf("done\n");
  term_t dict = PL_new_term_ref();
  if(!PL_put_dict(dict, tag, k, keys, valterms)) return false;
  printf("dict done\n");
  return PL_unify_term(to, PL_TERM, dict);
  //term_t dict = PL_new_term_ref();
  //if(!PL_put_dict(dict, tag, k, keys, valterms)) return false;

  //return PL_unify(to, dict);
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
    printf("success\n");
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
  printf("PARSE: %s\n", at);
  char *after;
  if(!json_parse(at, to, &after)) return false;
  skipws(&after);
  return *after == 0;
}
