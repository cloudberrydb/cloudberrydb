#pragma once
#include "comm/cbdb_api.h"

#ifdef __cplusplus
extern "C" {
#endif
struct List;
extern struct List *paxc_raw_parse(const char *str);
extern struct List *paxc_parse_partition_ranges(const char *ranges);

#ifdef __cplusplus
}
#endif
