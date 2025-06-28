/* PostgreSQL Wire Protocol over TCP/IP socket */
#include <stdint.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <memory.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <netdb.h>
#include <string.h>
#include <stdbool.h>
#include "pgwire.h"
#include "util.h"

/* Read message from connection socket into *msg.
 * The payload data is stored in connection buffer.
 */
static bool read_msg(PgConn *c, PgMessage *msg) {
  char hdr[5];
  if(read(c->sockfd, hdr, 5) != 5) {
    err0("Could not read from socket.");
    return false;
  }
  msg->type = hdr[0];
  msg->len = ntohl(*((int32_t*)&hdr[1])) - 4;
  if(!pg_ensure_buf(c, msg->len)) return false;
  char *data = &c->buf[c->buf_pos];
  ssize_t r = read(c->sockfd, data, msg->len);
  if(r < msg->len) {
    err("Could not read from socket (%zd < %d).", r, msg->len);
    return false;
  }
  msg->data = c->buf_pos;
  msg->read = c->buf_pos;
  c->buf_pos += msg->len;
  return true;
}

bool read_startup_messages(PgConn *c) {
  bool auth_ok = false;
  PgMessage m;
  size_t buf_pos = c->buf_pos;
  while(m.type != 'Z') {
    if(!read_msg(c, &m)) return false;
    switch(m.type) {
    case 'R': { // auth status
      int status;
      get_i32(c, m.read, status);
      if(status == 0) auth_ok = true;
      break;
    }

    case 'K': // cancellation key data
      get_i32(c, m.read, c->pid);
      get_i32(c, m.read, c->secret_key);
      printf("cid: %d, secret key: %d\n", c->pid, c->secret_key);
      break;
      // S=parameter status, Z=ready for query
    case 'S':
      printf("%s = %s\n", &c->buf[m.data], &c->buf[m.data]+(strlen(&c->buf[m.data])+1));
      break;
    default: break;
    }
  }
  c->buf_pos = buf_pos; // discard messages
  return auth_ok;
}


static void extract_val(char **start, char *to) {
  char *at = *start;
  while(*at != 0 && *at != ' ') {
    *to = *at;
    to++; at++;
  }
  *to = 0;
  while(*at == ' ') at++; // skip the whitespace
  *start = at;
}

PgConn *pg_connect(char *conn_info) {
  struct sockaddr_in to;
  memset(&to, 0, sizeof(to));

  to.sin_family = AF_INET;
  to.sin_port = htons(5432);

  char *ci = conn_info;
  while(*ci) {
    if(strncmp(ci, "host=", 5)==0) {
      char host[128];
      ci += 5;
      extract_val(&ci, host);
      printf("host: %s\n", host);
      struct hostent *h = gethostbyname(host);
      if(h->h_addrtype == AF_INET) {
        to.sin_addr = *((struct in_addr **)h->h_addr_list)[0];
      } else {
        err("Unable to resolve host %s, got type %d", host, h->h_addrtype);
        return NULL;
      }
    } else if(strncmp(ci, "port=", 5)==0) {
      int port = 0;
      ci += 5;
      while(*ci >= '0' && *ci <= '9') {
        port = (port * 10) + (*ci - '0');
        ci++;
      }
      while(*ci == ' ') ci++;
      if(port == 0) {
        err0("Unable to extract port");
        return NULL;
      }
      printf("port: %d\n", port);
      to.sin_port = htons(port);
    } else {
      err("Unsupported connection info: %s", ci);
      return NULL;
    }
  }

  // connect and send startup message, expect auth ok response
  int sockfd = socket(AF_INET, SOCK_STREAM, 0);
  if(sockfd < 0) {
    err0("couldn't create socket");
    return NULL;
  }
  if(connect(sockfd, (struct sockaddr *)&to, sizeof(to)) < 0) {
    err0("connect failed");
    return NULL;
  }

  int len = 4+4+5+5+9+5+1;// int32, int32, "user\0", "xtdb\0", "database\0", "xtdb\0", \0
  char buf[len];
  *((int32_t*)&buf[0]) = htonl(len);
  *((int32_t*)&buf[4]) = htonl(196608); // protocol version
  memcpy(&buf[8], "user\0xtdb\0database\0xtdb\0", 24);
  buf[32] = 0;
  write(sockfd, buf, len);

  PgConn *c = malloc(sizeof(PgConn));
  c->sockfd = sockfd;

  // last thing, malloc a buffer for writing/reading
  c->buf = malloc(MIN_BUFFER_SIZE);
  if(c->buf == NULL) goto fail;
  c->buf_pos = 0;
  c->buf_size = MIN_BUFFER_SIZE;
  if(!read_startup_messages(c)) goto fail;
  return c;

 fail:
  if(c->buf) free(c->buf);
  close(sockfd);
  free(c);
  return NULL;
}

void pg_close(PgConn *c) {
  close(c->sockfd);
  free(c->buf);
  free(c);
}

bool pg_ensure_buf(PgConn *c, size_t extra) {
  size_t wanted = c->buf_pos + extra;
  size_t size = c->buf_size;
  if(wanted < size) {
    size_t new_size = c->buf_size * BUFFER_INCREASE_FACTOR;
    size_t increase = new_size - size;
    if(increase < MIN_BUFFER_INCREASE) {
      new_size = size + MIN_BUFFER_INCREASE;
    } else if(increase > MAX_BUFFER_INCREASE) {
      new_size = size + MAX_BUFFER_INCREASE;
    }
    char *new_buf = realloc(c->buf, new_size);
    if(new_buf == NULL) {
      err("Unable to allocate more buffer space, at: %zu, need: %zu",
              size, new_size);
      return false;
    }
    c->buf = new_buf;
    c->buf_size = new_size;
  }
  return true;
}

/* Send current buffer */
static bool pg_send(PgConn *c) {
  if(!write(c->sockfd, c->buf, c->buf_pos)) {
    err("Unable to write %zu bytes to socket.", c->buf_size);
    return false;
  }
  c->buf_pos = 0; // reset buffer position
  return true;
}

/* put Parse message to buffer */
static bool put_parse(PgConn *c, const char* sql, int num_params, int *param_oids) {
  put_ch(c,'P');
  mark_len(c);
  put_ch(c,0); // no name for prepared statement
  put_string(c,sql);
  put_i16(c,num_params);
  for(int i=0;i<num_params;i++) put_i32(c,param_oids[i]);
  update_len(c);
  return true;
}

/* put Bind message to buffer */
static bool put_bind(PgConn *c, int num_params, char **param_data) {
  put_ch(c, 'B');
  mark_len(c);
  // 'B', i32 len, portal name (none), prepared name (none), i16 (num param formats: 0 (text)),
  // for each parameter:
  // - i32 len
  // - bytes
  // i16 result column format codes: 1
  // i16 result column format: 1 (binary)
  put_ch(c,0); // portal name (none)
  put_ch(c,0); // prepared stmt name (none)
  put_i16(c,0); // text format for all params (text)
  put_i16(c,num_params);
  for(int p=0;p<num_params;p++) {
    put_len_string(c,param_data[p]);
  }
  put_i16(c,1); // format for all results
  put_i16(c,1); // binary
  update_len(c);
  return true;
}

static bool put_describe_portal(PgConn *c) {
  put_ch(c, 'D');
  mark_len(c);
  put_ch(c, 'P');
  put_ch(c, 0); // empty named portal
  update_len(c);
  return true;
}

static bool put_execute(PgConn *c) {
  put_ch(c, 'E');
  mark_len(c);
  put_ch(c, 0); // empty named portal
  put_i32(c, 0); // unlimited rows
  update_len(c);
  return true;
}

static bool put_sync(PgConn *c) {
  put_ch(c, 'S');
  put_i32(c, 4); // length only
  return true;
}

static bool expect_msg(PgConn *c, char msg, int expected_size) {
  char hdr[5];
  if(read(c->sockfd, hdr, 5) != 5) {
    err0("Could not read from socket.");
    return false;
  }
  if(msg != hdr[0]) {
    err("Expected '%c' message from server, got %c.", msg, hdr[0]);
    return false;
  }
  int size = ntohl(*((int32_t*)&hdr[1]));
  if(expected_size != -1 && size != expected_size) {
    err("Unexpected size in '%c' message, expected %d, got: %d", msg, expected_size, size);
    return false;
  }
  // Read rest of message
  if(size > 4) {
    if(!pg_ensure_buf(c, size - 4)) return false;
    if(read(c->sockfd, &c->buf[c->buf_pos], size-4) != size-4) {
      err("Couldn't read %d bytes from socket.", size-4);
      return false;
    }
  }
  return true;
}

static bool expect_simple(PgConn *c, char msg) { return expect_msg(c, msg, 4); }

static bool expect_ready(PgConn *c) {
  char msg[6];
  if(read(c->sockfd, msg, 6) != 6) {
    err0("Could not read from socket.");
  }
  if('Z' != msg[0]) {
    err("Expected ready (Z) message, got: %c", msg[0]);
    return false;
  }
  int size = ntohl(*((int32_t*)&msg[1]));
  if(size != 5) {
    err("Unexpected size in ready message, expected 5, got: %d", size);
    return false;
  }
  return true;
}

void pg_clear(PgConn *c) { c->buf_pos = 0; }

/* Issue a query, sends parse and bind messages. */
PgResult pg_query(PgConn *c, const char* sql, int num_params, int *param_oids,
               char **param_data) {
  if(!put_parse(c, sql, num_params, param_oids)) goto fail;
  if(!put_bind(c, num_params, param_data)) goto fail;
  if(!put_describe_portal(c)) goto fail;
  if(!put_execute(c)) goto fail;
  if(!put_sync(c)) goto fail;
  if(!pg_send(c)) goto fail;

  c->buf_pos = 0;

  // expect ParseComplete ('1') and BindComplete ('2') messages
  if(!expect_simple(c, '1')) goto fail;
  if(!expect_simple(c, '2')) goto fail;

  PgMessage msg;
  if(!read_msg(c, &msg)) goto fail;
  if(msg.type == 'n') {
    /* got NoData, this executed ok */
    if(!expect_msg(c, 'C', -1)) goto fail; // expect command complete
    if(!expect_ready(c)) goto fail;
    return (PgResult) { true, 0, 0, 0 };
  } else if(msg.type != 'T') {
    err("Expected RowDescription ('B') message, got: %c", msg.type);
    goto fail;
  }
  if(msg.len < 2) {
    err("Expected RowDescription len >= 2, got: %u", msg.len);
    goto fail;
  }

  PgResult res = (PgResult) { true, msg.data, 0, -1 };
  int pos = msg.data;
  get_i16(c, pos, res.fields);

  return res;

 fail:
  c->buf_pos = 0;
  return (PgResult) { false, 0, 0 };
}

bool pg_field(PgConn *c, PgResult res, int field_num, int *oid_out, char **name_out) {
  if(field_num < 0 || field_num >= res.fields) {
    return false;
  } else {
    size_t pos = 2;
    char *name;
    int _tbl, _attr, oid, _typlen, _typmod, _fmt;

    for(int i=0;i<=field_num;i++) {
      // read the oid
      get_string(c, pos, name);
      get_i32(c, pos, _tbl);
      get_i16(c, pos, _attr);
      get_i32(c, pos, oid);
      get_i16(c, pos, _typlen);
      get_i32(c, pos, _typmod);
      get_i16(c, pos, _fmt);
    }
    *oid_out = oid;
    *name_out = name;
    return true;
  }
}

PgRow pg_next_row(PgConn *c, PgResult *res) {
  if(res->row_start != -1) {
    // invalidate old row, read on top of it
    c->buf_pos = res->row_start;
  }
  PgMessage m;
  if(!read_msg(c, &m)) goto fail;
  if(m.type == 'D') {
    // got DataRow
    res->row_start = m.data;
    return (PgRow) { true, true };
  } else if(m.type == 'C') {
    // CommandComplete
    if(!expect_ready(c)) goto fail;
    return (PgRow) { true, false };
  } else {
    err("Unexpected message from server: %c", m.type);
    goto fail;
  }

 fail:
  return (PgRow) { false, false };
 }


PgVal pg_value(PgConn *c, PgResult *res, int field) {
  size_t pos = res->row_start;
  if(pos == -1) goto fail;
  short fields;
  get_i16(c, pos, fields);
  if(field < 0 || field >= fields) goto fail;
  PgVal v;
  int len;
  for(int i=0;i<=field;i++) {
    get_i32(c, pos, len);
    if(len == -1) {
      v.is_null = true;
      v.len = 0;
      v.data = NULL;
    } else {
      v.is_null = false;
      v.len = len;
      v.data = &c->buf[pos];
      pos += len;
    }
  }
  v.success = true;
  return v;
 fail:
    return (PgVal) { false, false, 0, NULL };
}

/*
int main(int argc, char *argv[]) {

  PgConn *c = pg_connect("host=localhost port=5433");

  int oids[] = { 20 };
  char *values[] = { "1" };

  PgResult res = pg_query(c, "SELECT *, 'customer' as \"@type\" FROM customer a
WHERE a._id > $1 ", 1, oids, values); if(res.success) {

    printf("success!");
    for(int i=0;i<res.fields;i++) {
      int oid;
      char *name;
      pg_field(c, res, i, &oid, &name);
      printf(" GOT Field %d: %s (type %d)\n", i, name, oid);
    }

    PgRow r;
    for(;;) {
      r = pg_next_row(c, &res);
      if(!r.has_row) break;
      printf("got row!\n");
      for(int i=0;i<res.fields; i++) {
        PgVal v = pg_value(c, &res, i);
        if(!v.success) break;
        if(v.is_null) { printf("col %d is NULL\n", i); }
        else { printf("col %d has data len: %zu => %s\n", i, v.len, v.data); }
      }
    }


  }

  return 0;
}
*/
