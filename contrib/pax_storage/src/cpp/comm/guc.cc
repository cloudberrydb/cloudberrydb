#include "comm/guc.h"

namespace pax {
bool pax_enable_debug = true;

int pax_scan_reuse_buffer_size = 0;

#ifdef ENABLE_PLASMA
bool pax_enable_plasma_in_mem = true;
#endif

}  // namespace pax
