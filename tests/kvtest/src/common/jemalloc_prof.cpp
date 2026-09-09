#include "jemalloc_prof.h"

#ifdef KVTEST_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

bool JemallocProfSupported()
{
#ifdef KVTEST_JEMALLOC
    bool supported = false;
    size_t size = sizeof(supported);
    return mallctl("config.prof", &supported, &size, nullptr, 0) == 0 && supported;
#else
    return false;
#endif
}
