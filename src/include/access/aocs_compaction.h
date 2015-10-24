/*------------------------------------------------------------------------------
 *
 * aocs_compaction
 *
 * Copyright (c) 2013, Pivotal.
 *
 *------------------------------------------------------------------------------
*/
#ifndef AOCS_COMPACTION_H
#define AOCS_COMPACTION_H

#include "postgres.h"
#include "nodes/pg_list.h"
#include "utils/rel.h"

extern void AOCSDrop(Relation aorel,
		List* compaction_segno);
extern void AOCSCompact(Relation aorel, 
		List *compaction_segno_list,
		int insert_segno,
		bool isFull);
extern void AOCSTruncateToEOF(Relation aorel);
#endif
