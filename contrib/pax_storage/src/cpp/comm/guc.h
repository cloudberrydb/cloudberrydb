#pragma once

namespace pax {
extern bool pax_enable_debug;
extern bool pax_enable_filter;
extern int pax_scan_reuse_buffer_size;
extern int pax_max_tuples_per_group;

extern int pax_max_tuples_per_file;
extern int pax_max_size_per_file;

#ifdef VEC_BUILD
// The guc define in vectorization(contrib/vectorization/main.c:L31)
// PAX needs this GUC value to determine record batch return size.
// Since the GUC is not defined in CBDB, we need use GetConfigOptionByName to
// obtain the GUC. When vectorization changes the name of the GUC, PAX also
// needs to be changed accordingly.
#define VECTOR_MAX_BATCH_SIZE_GUC_NAME "vector.max_batch_size"
#endif

#ifdef ENABLE_PLASMA
extern bool pax_enable_plasma_in_mem;
#endif

}  // namespace pax

namespace paxc {
extern void DefineGUCs();
}
