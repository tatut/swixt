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

static bool from_db_value(term_t to, PGresult *res, size_t row, size_t field) {
  if (PQgetisnull(res, row, field)) {
    return PL_put_nil(to);
  }
  switch (PQftype(res, field)) {
    /*
     *    typname   |  oid
     -------------+-------
     _int8       |  1016
     float8      |   701
     bytea       |    17
     date        |  1082
     float4      |   700
     numeric     |  1700
     int2        |    21
     jsonb       |  3802
     time        |  1083
     timestamptz |  1184
     _int4       |  1007
     int4        |    23
     int8        |    20
     transit     | 16384
     tstz-range  |  3910
     keyword     | 11111
     regproc     |    24
     _text       |  1009
     interval    |  1186
     varchar     |  1043
     uuid        |  2950
     json        |   114
     timestamp   |  1114
     boolean     |    16
     text        |    25
     regclass    |  2205
    */

  case 114:
    return json_parse_toplevel(PQgetvalue(res, row, field), to);

  default:
    return PL_put_string_chars(to, PQgetvalue(res, row, field));
  }
}

static foreign_t pl_query(term_t conn_handle, term_t query, term_t args, term_t RESULT) {
  PGresult *res;
  PGconn *conn;
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
      if(!PL_is_compound(argval)) goto free_args_fail;
      if(!PL_get_arg(1, argval, arg_oid)) goto free_args_fail;
      if(!PL_get_arg(2, argval, arg_str)) goto free_args_fail;
      if (PL_get_chars(arg_str, &s, CVT_ALL | REP_UTF8)) {
        uint64_t oid;
        if(!PL_get_uint64(arg_oid, &oid)) goto free_args_fail;
        printf("arg %d: %s (OID: %llu)\n", argc, s, oid);
        query_args[argc] = malloc(strlen(s) + 1);
        query_arg_types[argc] = (const Oid) oid;
        strcpy((char*)query_args[argc], s);
        argc++;
      }

    }
    if(!PL_get_tail(arg, arg)) break;
  }
  if(PL_get_chars(query, &s, CVT_ALL|REP_UTF8)) {
    printf("thread(%d) running query: %s (argc: %d)\n", PL_thread_self(), s,
           argc);
    res = PQexecParams(conn, s, argc, query_arg_types, query_args,
                       NULL, NULL, 1);
    if(PQresultStatus(res) == PGRES_TUPLES_OK) {
      PL_fid_t fid = PL_open_foreign_frame();

      printf("pq result ok! %d\n", PQntuples(res));
      size_t nfields = PQnfields(res);

      term_t result = PL_new_nil_ref();

      //PL_cons_list(result, PL_new_nil_ref(), result);
      int row = PQntuples(res)-1;
      atom_t table_tag = PL_new_atom("fixme"); // use __type field
      atom_t field_tags[nfields];
      for(size_t f=0;f<nfields;f++) {
        field_tags[f] = PL_new_atom(PQfname(res, f));
        printf("field %ld type is %d\n", f, PQftype(res, f));
      }
      while(row >= 0) {
        term_t dict = PL_new_term_ref();
        term_t vals = PL_new_term_refs(nfields);
        for (size_t f = 0; f < nfields; f++) {
          if(!from_db_value((vals+f), res, row, f)) goto free_args_fail;

        }
        PL_put_dict(dict, table_tag, nfields, field_tags, vals);
        PL_cons_list(result, dict, result);
        row--;
      }
      //FIXME: free query args
      return PL_unify(result, RESULT);
    } else {
      fprintf(stderr, "Query failed: %s", PQresultErrorMessage(res));

      return false;
    }
  }

 free_args_fail:
  for(int i=0;i<argc;i++) free((void*)query_args[i]);
  if(res != NULL) PQclear(res);
  return false;
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
