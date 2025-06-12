#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "libpq-fe.h" /* libpq */
#include "SWI-Prolog.h"
#include "SWI-Stream.h"
#include <stdbool.h>

PGconn *conn; // the global connection

void type_name(PGconn *c, Oid oid, char *to) {
  PGresult *res;
  char query[64];
  snprintf(query, 64, "SELECT typname FROM pg_type WHERE oid = %d", oid);
  res = PQexec(c, query);
  if(PQresultStatus(res) != PGRES_TUPLES_OK) {
      strcpy(to, "ERROR\0");
  } else if(PQntuples(res) < 1) {
    strcpy(to, "N/A\n");
  } else {
    strcpy(to, PQgetvalue(res, 0, 0));
  }
  PQclear(res);
}

static foreign_t pl_connect(term_t connstr) {
  char *s;
  if(PL_get_chars(connstr, &s, CVT_ALL|REP_UTF8)) {
    conn = PQconnectdb(s);
    if(PQstatus(conn) != CONNECTION_OK) {
      fprintf(stderr, "Connection failed with: %s\n", s);
      return false;
    }
    return true;
  } else {
    return false;
  }
}

#define MAX_ARGS 32

static foreign_t pl_query(term_t query, term_t args, term_t RESULT) {
  PGresult *res;
  char *s;
  const char *query_args[MAX_ARGS];
  Oid query_arg_types[MAX_ARGS];
  int argc=0;
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
    printf("running query: %s (argc: %d)\n", s, argc);
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
        field_tags[f] = PL_new_atom(PQfname(res,f));
      }
      while(row >= 0) {
        term_t dict = PL_new_term_ref();
        term_t vals = PL_new_term_refs(nfields);
        for (size_t f = 0; f < nfields; f++) {
          PL_put_string_chars((vals+f), PQgetvalue(res, row, f));
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

install_t install_swixt(void) {
  PL_register_foreign("swixt_pg_connect", 1, pl_connect, 0);
  PL_register_foreign("swixt_pg_query", 3, pl_query, 0);
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
