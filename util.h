#ifndef util_h
#define util_h

#include "SWI-Stream.h"

#ifdef DEBUG
#define dbg(fmt, args...)                                                      \
  { Sdprintf(fmt "\n", args); }

#endif

#ifndef DEBUG
#define dbg(args...)
#endif

#define err(fmt, args...)                                                      \
  { Sfprintf(Serror, fmt "\n", args); }

#define err0(fmt)                                                              \
  { Sfprintf(Serror, fmt "\n"); }

#endif
