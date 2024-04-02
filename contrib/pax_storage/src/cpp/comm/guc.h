#pragma once

namespace pax {
extern bool pax_enable_debug;
extern bool pax_enable_filter;
extern int pax_scan_reuse_buffer_size;
extern int pax_max_tuples_per_group;

extern int pax_max_tuples_per_file;
extern int pax_max_size_per_file;

#ifdef ENABLE_PLASMA
extern bool pax_enable_plasma_in_mem;
#endif

}  // namespace pax

namespace paxc {
extern void DefineGUCs();
}
