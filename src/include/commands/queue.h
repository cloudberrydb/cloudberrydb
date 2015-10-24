/*-------------------------------------------------------------------------
 *
 * queue.h
 *	  Commands for manipulating resource queues.
 *
 * Copyright (c) 2006-2010, Greenplum inc.
 *
 * $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
#ifndef QUEUE_H
#define QUEUE_H

#include "nodes/parsenodes.h"


extern List * 
GetResqueueCapabilityEntry(Oid  queueid);
extern void CreateQueue(CreateQueueStmt *stmt);
extern void AlterQueue(AlterQueueStmt *stmt);
extern void DropQueue(DropQueueStmt *stmt);
extern char *GetResqueueName(Oid resqueueOid);
extern char *GetResqueuePriority(Oid queueId);

#endif   /* QUEUE_H */
