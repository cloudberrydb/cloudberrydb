#pragma once

namespace pax {
extern bool pax_enable_debug;
extern int pax_scan_reuse_buffer_size;
extern bool pax_allow_oper_fallback;

#ifdef ENABLE_PLASMA
extern bool pax_enable_plasma_in_mem;
#endif

}  // namespace pax
