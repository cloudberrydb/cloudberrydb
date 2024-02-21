#ifndef SORTED_MERGE_C_H
#define SORTED_MERGE_C_H

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"

struct List;

void *createSortedMerge(char *filename, List *readers);
bool sortedMergeNext(void *sortedMerge, int64 *value);
void sortedMergeClose(void *sortedMerge);

#ifdef __cplusplus
}
#endif

#endif // SORTED_MERGE_C_H
