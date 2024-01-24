#include "comm/guc.h"

namespace pax {
bool pax_enable_debug = true;
int pax_scan_reuse_buffer_size = 0;
bool pax_allow_oper_fallback = false;

#ifdef ENABLE_PLASMA
bool pax_enable_plasma_in_mem = true;
#endif

}  // namespace pax
