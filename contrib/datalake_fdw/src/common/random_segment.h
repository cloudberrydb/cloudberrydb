#ifndef DATALAKE_RANDOM_SEGMENT_H
#define DATALAKE_RANDOM_SEGMENT_H

#include "../datalake_def.h"

extern List* select_random_segments(int num_nodes, int random_num);

extern void exec_segment(List* selectSegments, int cursegid, int cursegnum,
						 bool *exec, int *segindex, int *segnum);

#endif
