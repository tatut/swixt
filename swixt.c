#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "pgwire.h"
#include "SWI-Prolog.h"
//#include "SWI-Stream.h"
#include <stdbool.h>
#include "json.h"
#include "util.h"

static foreign_t pl_connect(term_t connstr, term_t CONN) {
  char *s;
  if(PL_get_chars(connstr, &s, CVT_ALL|REP_UTF8)) {
    PgConn *conn = pg_connect(s);
    if(!conn) {
      err("Connection failed with: %s", s);
      return false;
    }
    functor_t c = PL_new_functor(PL_new_atom("xtconn"), 1);
    term_t c1 = PL_new_term_ref();
    term_t xtconn = PL_new_term_ref();
    if (!PL_put_int64(c1, (uint64_t)conn)) goto fail;
    if (!PL_cons_functor(xtconn, c, c1))
      goto fail;
    return PL_unify(xtconn, CONN);
  fail:
    if(conn) pg_close(conn);
    return false;
  } else {
    return false;
  }
}

PgConn *get_connection(term_t conn_handle) {
  term_t pointer = PL_new_term_ref();
  if (!PL_get_arg(1, conn_handle, pointer)) {
    term_t except = PL_unify_term(except, PL_FUNCTOR_CHARS, "invalid_xtconn");
    PL_raise_exception(except);
    return NULL;
  }
  PgConn *conn;
  if (!PL_get_uint64_ex(pointer, (uint64_t *)&conn))
    return NULL;
  return conn;
}

static foreign_t pl_close(term_t conn_handle) {
  PgConn *conn = get_connection(conn_handle);
  if (conn == NULL) {
    return false;
  } else {
    pg_close(conn);
    return true;
  }
}

#define MAX_ARGS 32

static bool from_db(term_t t, char *data, size_t len, int type) {
  //printf("from db: %zu, oid: %d\n", len, type);
  union {
    uint64_t int_val;
    double double_val;
  } dbl;

  switch (type) {
  case 20: // int8
    return PL_put_int64(t, htonll(*((int64_t *)data)));
  case 23: // int4
    return PL_put_int64(t, htonl(*((int32_t*)data)));
  case 25: // text
    return PL_put_chars(t, PL_STRING|REP_UTF8, len, data);
  case 114: // json
    return json_parse_toplevel(data, t);
  case 701: // float8
    memcpy(&dbl.int_val, data, 8);
    dbl.int_val = htonll(dbl.int_val);
    return PL_put_float(t, dbl.double_val);
  default:
    return PL_put_chars(t, PL_STRING|REP_UTF8, len, data);
  }
}

static bool from_db_array(term_t to, char *data) {
  // 20 byte header: ndim, has_null, element_type, dim_size, lower_bound (all
  // int32)
  int ndim = ntohl(*((int32_t *)data));
  if (ndim != 1) {
    err("Only 1 dimensional arrays are supported at the moment, ndim: %d", ndim);
    return false;
  }
  int has_null = ntohl(*((int32_t *)(data + 4)));
  int element_type = ntohl(*((int32_t *)(data + 8)));
  int dim_size = ntohl(*((int32_t *)(data + 12)));
  int lower_bound = ntohl(*((int32_t *)(data + 16)));

  term_t list = PL_copy_term_ref(to);
  term_t item = PL_new_term_ref();
  data += 20; // skip 20 byte header
  for(int i=0;i<dim_size;i++) {
    int len = ntohl(*((int32_t *)data));
    term_t v = PL_new_term_ref();
      if (len == -1) {
        if(!PL_put_atom_chars(v, "nil")) goto fail;
      } else {
        if (!from_db(v, data+4, len, element_type))
          goto fail;
      }
      if(!PL_unify_list(list, item, list) || !PL_unify(item, v))
        goto fail;
      data += 4 + len;
  }
  return PL_unify_nil(list);
 fail:
  return false;
}

static bool from_db_value(term_t to, PgVal res, int type) {
  int64_t num;
  if (res.is_null) {
    return PL_put_atom_chars(to, "nil");
  }


  switch (type) {
    // array types
  case 1007: // int4
  case 1009: // text
  case 1016: // int8
    return from_db_array(to, res.data);

    // regular value
  default:
    return from_db(to, res.data, res.len, type);
  }
}

static foreign_t pl_query(term_t conn_handle, term_t query, term_t args,
                          term_t RESULT) {
#define fail() { success = false; goto end; }
  printf("do the query!\n");
  PgResult res;
  PgConn *conn;
  bool success;
  char *s;
  char *query_args[MAX_ARGS];
  int query_arg_types[MAX_ARGS];
  int argc = 0;
  conn = get_connection(conn_handle);
  dbg("got connection %llu", (uint64_t) conn);
  if(conn == NULL) return false;
  if(!PL_is_list(args)) {
    err0("Query arguments is not a list");
    return false;
  }
  term_t arg = args;
  term_t argval = PL_new_term_ref();
  term_t arg_oid = PL_new_term_ref();
  term_t arg_str = PL_new_term_ref();
  dbg("start adding args\n");
  term_t head = PL_new_term_ref();
  term_t tail = PL_copy_term_ref(args);
  while(PL_get_list_ex(tail, head, tail)) {
    if(!PL_is_compound(head)) fail();
    if(!PL_get_arg(1, head, arg_oid)) fail();
    if(!PL_get_arg(2, head, arg_str)) fail();
    if(!PL_get_chars(arg_str, &s, CVT_ALL | REP_UTF8)) fail();

    uint64_t oid;
    if(!PL_get_uint64(arg_oid, &oid)) fail();
    dbg("arg %d: %s (OID: %llu)", argc, s, oid);
    query_args[argc] = malloc(strlen(s) + 1);
    query_arg_types[argc] = oid;
    strcpy((char*)query_args[argc], s);
    argc++;
  }
  printf("done with args\n");
  if(PL_get_chars(query, &s, CVT_ALL|REP_UTF8)) {
    dbg("thread(%d) running query: %s (argc: %d)", PL_thread_self(), s,
         argc);
    res = pg_query(conn, s, argc, query_arg_types, query_args);
    if(!res.success) fail();

    if(res.fields) {

      dbg("pq result ok!\n");
      size_t nfields = res.fields;

      term_t result = PL_copy_term_ref(RESULT);

      size_t tag_field = -1;
      atom_t table_tag = 0;
      atom_t field_tags[nfields];
      int types[nfields];

      for (size_t f = 0; f < nfields; f++) {
        char *name;
        if(!pg_field(conn, res, f, &types[f], &name)) fail();
        if (strcmp(name, "@type") == 0) {
          tag_field = f;
        } else {
          field_tags[f] = PL_new_atom(name);
        }
      }
      PgRow row = pg_next_row(conn, &res);
      term_t item = PL_new_term_ref();
      while(row.has_row) {
        term_t dict = PL_new_term_ref();

        term_t vals = PL_new_term_refs(nfields - (tag_field == -1 ? 0 : 1));
        size_t ref = 0;
        for (size_t f = 0; f < nfields; f++) {
          PgVal v = pg_value(conn, &res, f);
          if (f == tag_field) {
            if(v.is_null) fail();
            table_tag = PL_new_atom_nchars(v.len, v.data);
          } else {
            if (!from_db_value((vals + ref), v, types[f]))
              fail();
            ref++;
          }
        }
        if(!PL_put_dict(dict, table_tag, nfields - (tag_field == -1 ? 0 : 1), field_tags, vals)) return false;

        if(!PL_unify_list(result, item, result) || !PL_unify(item, dict)) fail();
        row = pg_next_row(conn, &res);
      }
      success = PL_unify_nil(result);
    } else if (res.success) {
      return true;
    } else {
      err0("Query failed.");
      fail();
    }
  }

 end:
  for(int i=0;i<argc;i++) free((void*)query_args[i]);
  pg_clear(conn);
  return success;
#undef fail
}

foreign_t testparse(term_t in, term_t parsed) {
  char *s;
  if (PL_get_chars(in, &s, CVT_ALL | REP_UTF8)) {
    return json_parse_toplevel(s, parsed);
  }
  return false;
}

install_t install_swixt(void) {
  PL_register_foreign("swixt_pg_connect", 2, pl_connect, 0);
  PL_register_foreign("swixt_pg_close", 1, pl_close, 0);
  PL_register_foreign("swixt_pg_query", 4, pl_query, 0);
  PL_register_foreign("swixt_json", 2, testparse, 0);
}
