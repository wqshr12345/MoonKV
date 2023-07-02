#pragma once

#include <stdlib.h>

namespace ROCKSDB_NAMESPACE {

#ifdef NVM_PREFETCH
#define prefetch(x) __builtin_prefetch(x, 0, 1)
#define PREFETCH_STRIDE 64
#endif

static inline void prefetch_range(void *addr, size_t len)
{
#ifdef NVM_PREFETCH
  char *cp;
  char *end = (char*)addr + len;

  for (cp = (char*)addr; cp < end; cp += PREFETCH_STRIDE)
    prefetch(cp);
#endif
}


}