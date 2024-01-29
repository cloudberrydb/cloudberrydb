#include "comm/guc.h"

#include "storage/pax_defined.h"

namespace pax {
bool pax_enable_debug = true;
int pax_scan_reuse_buffer_size = 0;
int pax_max_tuples_per_group = VEC_BATCH_LENGTH;

#ifdef ENABLE_PLASMA
bool pax_enable_plasma_in_mem = true;
#endif

}  // namespace pax
