#ifndef util_h
#define util_h

#include "SWI-Stream.h"

#ifdef DEBUG
#define dbg(fmt, args...)                                                      \
  { Sdprintf(fmt "\n", args); }
#define dbg0(msg)                                                              \
  { Sdprintf(msg "\n"); }

#endif

#ifndef DEBUG
#define dbg(args...)
#define dbg0(msg)
#endif

#define err(fmt, args...)                                                      \
  { Sfprintf(Serror, fmt "\n", args); }

#define err0(msg)                                                              \
  { Sfprintf(Serror, msg "\n"); }

#endif
