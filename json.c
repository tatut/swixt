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
 * Parsing may modify the JSON input char* to avoid allocating extra
 * memory.
 */
#include "SWI-Prolog.h"
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include "json.h"
#include "util.h"

#define expect(expr)                                                           \
  if (!(expr))                                                          \
  return false


void skipws(char **at) {
  char c = **at;
  while(c == ' ' || c == '\t' || c == '\n' || c == '\r') {
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

static int hex(char ch) {
  if(ch >= '0' && ch <= '9') return ch - '0';
  if(ch >= 'A' && ch <= 'F') return ch - ('A' - 10);
  if(ch >= 'a' && ch <= 'f') return ch - ('a' - 10);
  return -1;
}

static bool parse_string(char *at, term_t to, char **after) {
  char *r, *w;
  // Naive first attempt, just take bytes until '"'
  char *end = at+1;
  while(*end != '"') {
    if(*end == '\\') goto handle_escape;
    end++;
  }
  *after = end+1;
  return PL_unify_string_nchars(to, end-at-1, at+1);

 handle_escape:
  /* handle escapes, mutates input char*, keep track of read and write
   * pointers.
   */
  r = end;
  w = end;
  while(*r != '"') {
    if(*r == '\\') {
      r++;
      switch(*r) {
      case '"': r++; *w = '"'; w++; break;
      case 't': r++; *w = '\t'; w++; break;
      case 'n': r++; *w = '\n'; w++; break;
      case '\\':r++; *w = '\\'; w++; break;
      case '/': r++; *w = '/'; w++; break;
      case 'b': r++; *w = '\b'; w++; break;
      case 'f': r++; *w = '\f'; w++; break;
      case 'u': {
        char hex[5] = { *(r+1), *(r+2), *(r+3), *(r+4), 0 };
        char *_end;
        long codepoint = strtol(hex, &_end, 16);

        if(codepoint <= 127) {
          // single utf-8 byte
          *w = codepoint;
          w++;
        } else if(codepoint <= 2047) {
          // 2 bytes
          *w = 0b11000000 + (0b00011111 & (codepoint>>6));
          w++;
          *w = 0b10000000 + (0b00111111 & codepoint);
        } else if(codepoint <= 65535) {
          *w = 0b11100000 + (0b00001111 & (codepoint>>12));
          w++;
          *w = 0b10000000 + (0b00111111 & (codepoint>>6));
          w++;
          *w = 0b10000000 + (0b00111111 & codepoint);
          w++;
        } else if(codepoint <= 1114111) {
          *w = 0b11110000 + (0b00000111 & (codepoint>>18));
          w++;
          *w = 0b10000000 + (0b00111111 & (codepoint>>12));
          w++;
          *w = 0b10000000 + (0b00111111 & (codepoint>>6));
          w++;
          *w = 0b10000000 + (0b00111111 & codepoint);
          w++;
        }
        r += 5;

      }
      }
    } else {
      *w = *r;
      w++; r++;
    }
  }
  size_t len = w-at-1;
  *after = r+1;
  term_t str = PL_new_term_ref();
  if(!PL_put_chars(str, PL_STRING|REP_UTF8, len, at+1)) return false;
  return PL_unify(str, to);

 fail:
  return false;

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

static bool parse_datepart(char *at, long *year, long *month, long *day, char **after) {
  expect(read_int(at, year, &at));
  expect(*at == '-'); at++;
  expect(read_int(at, month, &at));
  expect(*at == '-'); at++;
  expect(read_int(at, day, &at));
  *after = at;
  return true;
}

static bool parse_timepart(char *at, long *hour, long *minute, long *seconds, long *micros, char **after) {
  *seconds = 0;
  *micros = 0;
  expect(read_int(at, hour, &at));
  expect(*at == ':'); at++;
  expect(read_int(at, minute, &at));
  if(*at == 0) goto end;
  expect(*at == ':'); at++;
  expect(read_int(at, seconds, &at));
  if(*at == 0) goto end;
  expect(*at == '.'); at++;
  expect(read_int(at, micros, &at));
 end:
  *after = at;
  return true;
}

static bool parse_timestamp(term_t to, char *at) {
  long year, month, day, hour, minute, seconds=0, micros=0;
  // 2025-06-14T17:45:12.666420 (seconds and micros optional)
  if(!parse_datepart(at, &year, &month, &day, &at)) return false;
  expect(*at == 'T'); at++;
  dbg("parsed datepart, timepart: %s", at);
  if(!parse_timepart(at, &hour, &minute, &seconds, &micros, &at)) return false;
  dbg("parsed timepart, rest: %s", at);
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

static bool parse_date(term_t to, char *at) {
  long year, month, day;
  if(!parse_datepart(at, &year, &month, &day, &at)) return false;
  expect(*at == 0);
  return PL_unify_term(to,
                       PL_FUNCTOR_CHARS, "date", 3,
                       PL_LONG, year, PL_LONG, month, PL_LONG, day);
}

static bool parse_time(term_t to, char *at) {
  long hour,minute,seconds,micros;
  if(!parse_timepart(at, &hour, &minute, &seconds, &micros, &at)) return false;
  expect(*at == 0);
  return PL_unify_term(to, PL_FUNCTOR_CHARS, "time", 4,
                       PL_LONG, hour, PL_LONG, minute,
                       PL_LONG, seconds, PL_LONG, micros);
}

static bool parse_special(term_t to, char *type, char *value) {
  if(!(type[0] == 'x' && type[1] == 't' && type[2] == ':')) goto fail;
  if(strcmp(type+3, "timestamp")==0) {
    return parse_timestamp(to, value);
  } else if(strcmp(type+3, "date")==0) {
    return parse_date(to, value);
  } else if(strcmp(type+3, "time")==0) {
    return parse_time(to, value);
  } else if(strcmp(type+3, "uuid")==0) {
    term_t uuid = PL_new_term_ref();
    if(!PL_put_string_chars(uuid, value)) return false;
    // PENDING: should use 16 byte compound term instead of string?
    // string is way more human readable, but a little longer
    return PL_unify_term(to, PL_FUNCTOR_CHARS, "uuid", 1,
                         PL_TERM, uuid);
  }
 fail:
  err("Unrecognized special @type: %s", type);
  return false;

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
      err("Too many object values, can't have more than %d", MAX_OBJECT);
      return false;
    }
    term_t key, val;

    expect(*at == '"'); // keys must be strings
    char keyname[MAX_KEY_LEN];
    if(!read_str(at, 128, keyname, &at)) return false;
    skipws(&at);
    expect(*at == ':'); at++; // must have ':' between key and value
    skipws(&at);

    // Have @type or @value special string value
    if(keyname[0] == '@' && *at == '"') {
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

  // If we got a @value, without @type, add that here
  if(has_value) {
    if(k == MAX_OBJECT) {
      err("Too many object values, can't have more than %d", MAX_OBJECT);
      return false;
    }
    keys[k] = PL_new_atom("@value");
    vals[k] = PL_new_term_ref();
    if(!PL_put_chars(vals[k], PL_STRING|REP_UTF8, strlen(value), value)) return false;
    k++;
  }
  // construct the dict
  term_t valterms = PL_new_term_refs(k);
  for(size_t i=0; i<k; i++) {
    if(!PL_unify((valterms+i),vals[i])) return false;
  }
  term_t dict = PL_new_term_ref();
  if(has_type) tag = PL_new_atom(type);
  if(!PL_put_dict(dict, tag, k, keys, valterms)) return false;
  return PL_unify(to, dict);

}


static bool parse_list(char *at, term_t to, char **after) {
  term_t list = PL_copy_term_ref(to);
  term_t item = PL_new_term_ref();
  if(*at != '[') goto fail;
  at++;
  skipws(&at);
  bool first = true;
  while(*at != ']') {
    // parse this item, and recursively parse next
    if(!first) {
      if(*at != ',') goto fail;
      at++;
    }
    first = false;
    skipws(&at);
    if(!PL_unify_list(list, item, list)) goto fail;
    if(!json_parse(at, item, &at)) goto fail;
    skipws(&at);
  }
  // at end of list, set after and return nil ref
  *after = at + 1;
  return PL_unify_nil(list);
 fail:
    return false;
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
  dbg("PARSE: %s", at);
  if(!json_parse(at, to, &after)) return false;
  skipws(&after);
  dbg(" => OK");
  return *after == 0;
}
