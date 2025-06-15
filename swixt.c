#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "libpq-fe.h"
#include "SWI-Prolog.h"
//#include "SWI-Stream.h"
#include <stdbool.h>
#include "json.h"

static foreign_t pl_connect(term_t connstr, term_t CONN) {
  char *s;
  if(PL_get_chars(connstr, &s, CVT_ALL|REP_UTF8)) {
    PGconn *conn = PQconnectdb(s);
    if(PQstatus(conn) != CONNECTION_OK) {
      fprintf(stderr, "Connection failed with: %s\n", s);
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
    PQfinish(conn);
    return false;
  } else {
    return false;
  }

}

PGconn *get_connection(term_t conn_handle) {
  term_t pointer = PL_new_term_ref();
  if (!PL_get_arg(1, conn_handle, pointer)) {
    term_t except = PL_unify_term(except, PL_FUNCTOR_CHARS, "invalid_xtconn");
    PL_raise_exception(except);
    return NULL;
  }
  PGconn *conn;
  if (!PL_get_uint64_ex(pointer, (uint64_t *)&conn))
    return NULL;
  return conn;
}

static foreign_t pl_close(term_t conn_handle) {
  PGconn *conn = get_connection(conn_handle);
  if (conn == NULL) {
    return false;
  } else {
    PQfinish(conn);
    return true;
  }
}

#define MAX_ARGS 32

static bool from_db(term_t t, char *data, size_t len, Oid type) {
  printf("from db: %zu, oid: %d\n", len, type);
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

typedef struct Arr {
  term_t item;
  bool success;
} Arr;

static Arr from_db_array_(char *data, Oid type, size_t nitems) {
  if (nitems == 0) {
    return (Arr){PL_new_nil_ref(), true};
  } else {
    int len = ntohl(*((int32_t *)data));
    Arr rest = from_db_array_(data + 4 + len, type, nitems - 1);
    if(rest.success) {
      term_t item = PL_new_term_ref();
      if (len == -1) {
        if(!PL_put_atom_chars(item, "nil")) goto fail;
      } else {
        if (!from_db(item, data+4, len, type))
          goto fail;
      }
      term_t res = PL_new_term_ref();
      if (!PL_cons_list(res, item, rest.item))
        goto fail;
      return (Arr) { res, true };
    }
  }
 fail:
  return (Arr){0, false};
}

static bool from_db_array(term_t to, PGresult *res, size_t row, size_t field) {
  // 20 byte header: ndim, has_null, element_type, dim_size, lower_bound (all
  // int32)
  char *data = PQgetvalue(res, row, field);
  int ndim = ntohl(*((int32_t *)data));
  if (ndim != 1) {
    fprintf(stderr, "Only 1 dimensional arrays are supported at the moment, ndim: %d\n", ndim);
    return false;
  }
  int has_null = ntohl(*((int32_t *)(data + 4)));
  int element_type = ntohl(*((int32_t *)(data + 8)));
  int dim_size = ntohl(*((int32_t *)(data + 12)));
  int lower_bound = ntohl(*((int32_t *)(data + 16)));

  term_t values = PL_new_term_refs(dim_size);

  Arr arr = from_db_array_(data + 20, element_type, dim_size);
  if (arr.success) {
    if(!PL_unify(arr.item, to)) return false;

    /*printf("array ndim: %d, has_null: %d, element_type: %d, dim_size: %d, "
           "lower_bound: %d\n",
           ndim, has_null, element_type, dim_size, lower_bound);
    */
    return true;
  } else {
    return false;
  }

}
static bool from_db_value(term_t to, PGresult *res, size_t row, size_t field) {
  int64_t num;
  if (PQgetisnull(res, row, field)) {
    return PL_put_atom_chars(to, "nil");
  }
  Oid type = PQftype(res, field);

  switch (type) {
    // array types
  case 1007: // int4
  case 1009: // text
  case 1016: // int8
    return from_db_array(to, res, row, field);

    // regular value
  default:
    return from_db(to, PQgetvalue(res, row, field),
                   PQgetlength(res, row, field),
                   PQftype(res, field));
  }
}

static foreign_t pl_query(term_t conn_handle, term_t query, term_t args,
                          term_t RESULT) {
#define fail() { success = false; goto end; }
  PGresult *res;
  PGconn *conn;
  bool success;
  char *s;
  const char *query_args[MAX_ARGS];
  Oid query_arg_types[MAX_ARGS];
  int argc = 0;
  conn = get_connection(conn_handle);
  if(conn == NULL) return false;
  if(!PL_is_list(args)) {
    fprintf(stderr, "Query arguments is not a list\n");
    return false;
  }
  term_t arg = args;
  term_t argval = PL_new_term_ref();
  term_t arg_oid = PL_new_term_ref();
  term_t arg_str = PL_new_term_ref();
  while(PL_is_list(arg)) {
    if(PL_get_head(arg, argval)) {
      if(!PL_is_compound(argval)) fail();
      if(!PL_get_arg(1, argval, arg_oid)) fail();
      if(!PL_get_arg(2, argval, arg_str)) fail();
      if (PL_get_chars(arg_str, &s, CVT_ALL | REP_UTF8)) {
        uint64_t oid;
        if(!PL_get_uint64(arg_oid, &oid)) fail();
        //printf("arg %d: %s (OID: %llu)\n", argc, s, oid);
        query_args[argc] = malloc(strlen(s) + 1);
        query_arg_types[argc] = (const Oid) oid;
        strcpy((char*)query_args[argc], s);
        argc++;
      }

    }
    if(!PL_get_tail(arg, arg)) break;
  }
  if(PL_get_chars(query, &s, CVT_ALL|REP_UTF8)) {
    //printf("thread(%d) running query: %s (argc: %d)\n", PL_thread_self(), s,
    //     argc);
    res =
        PQexecParams(conn, s, argc, query_arg_types, query_args, NULL, NULL, 1);
    ExecStatusType status = PQresultStatus(res);
    if(status == PGRES_TUPLES_OK) {
      PL_fid_t fid = PL_open_foreign_frame();

      //printf("pq result ok! %d\n", PQntuples(res));
      size_t nfields = PQnfields(res);

      term_t result = PL_new_nil_ref();

      int row = PQntuples(res) - 1;
      size_t tag_field = -1;
      atom_t table_tag = 0;
      atom_t field_tags[nfields];
      for (size_t f = 0; f < nfields; f++) {
        if (strcmp(PQfname(res, f), "@type") == 0) {
          tag_field = f;
        } else {
          field_tags[f] = PL_new_atom(PQfname(res, f));
          //printf("field %ld type is %d\n", f, PQftype(res, f));
        }
      }
      while(row >= 0) {
        term_t dict = PL_new_term_ref();
        term_t vals = PL_new_term_refs(nfields - (tag_field == -1 ? 0 : 1));
        size_t ref = 0;
        for (size_t f = 0; f < nfields; f++) {
          if (f == tag_field) {
            //printf("table tag: %s\n", PQgetvalue(res, row, f));
            table_tag = PL_new_atom(PQgetvalue(res, row, f));
          } else {
            if (!from_db_value((vals + ref), res, row, f))
              fail();
            ref++;
          }
        }
        if(!PL_put_dict(dict, table_tag, nfields - (tag_field == -1 ? 0 : 1), field_tags, vals)) return false;
        if(!PL_cons_list(result, dict, result)) fail();
        row--;
      }
      success = PL_unify(result, RESULT);
    } else if (status == PGRES_COMMAND_OK) {
      return true;
    } else {
      fprintf(stderr, "Query failed: %s", PQresultErrorMessage(res));
      fail();
    }
  }

 end:
  for(int i=0;i<argc;i++) free((void*)query_args[i]);
  if(res != NULL) PQclear(res);
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
/*
int main(int argc, char**argv) {
  PGconn *conn;
  PGresult *res;
  int retval = 0;

  conn = PQconnectdb("host=localhost port=5432 dbname=xtdb");
  // Check to see that the backend connection was successfully made
  if (PQstatus(conn) != CONNECTION_OK) {
    fprintf(stderr, "%s", PQerrorMessage(conn));
    retval = 1;
    goto exit_con;
  }

  res = PQexec(conn, "SELECT * FROM customer");
  if (PQresultStatus(res) != PGRES_TUPLES_OK) {
      fprintf(stderr, "SET failed: %s", PQerrorMessage(conn));
      PQclear(res);
      retval = 2;
      goto exit_con;
  }

  // get all fields
  for(int f=0; f<PQnfields(res); f++) {
    char type[64];
    type_name(conn, PQftype(res,f), &type[0]);
    printf("Field: %s (type: %d => %s)\n", PQfname(res, f), PQftype(res, f), type);
  }

  // loop through results
  for(int i=0; i<PQntuples(res); i++) {

  }

  PQclear(res);

 exit_con:
  PQfinish(conn);

  return retval;
}
*/
