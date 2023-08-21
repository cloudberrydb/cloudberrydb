/*-------------------------------------------------------------------------
 *
 * pg_task.h
 *	  save all tasks of cron task scheduler.
 *
 * Portions Copyright (c) 2023, HashData Technology Limited.
 *
 *
 * IDENTIFICATION
 *	    src/include/catalog/pg_task.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_TASK_H
#define PG_TASK_H

#include "catalog/genbki.h"
#include "catalog/pg_task_d.h"

/* ----------------
 *		pg_task definition.  cpp turns this into
 *		typedef struct FormData_pg_task
 * ----------------
 */
CATALOG(pg_task,9637,TaskRelationId) BKI_SHARED_RELATION
{
	Oid			jobid;
	text		schedule;
	text		command;
	text		nodename;
	int32		nodeport;
	text		database;
	text		username;
	bool		active BKI_DEFAULT(t);
	text		jobname;
} FormData_pg_task;

typedef FormData_pg_task *Form_pg_task;

DECLARE_UNIQUE_INDEX(pg_task_jobname_username_index, 8915, on pg_task using btree(jobname text_ops, username text_ops));
#define TaskJobNameUserNameIndexId  8915
DECLARE_UNIQUE_INDEX_PKEY(pg_task_jobid_index, 8916, on pg_task using btree(jobid oid_ops));
#define TaskJobIdIndexId  8916

extern Oid TaskCreate(const char *schedule, const char *command,
					  const char *nodename, int32 nodeport,
					  const char *database, const char *username,
					  bool active, const char *jobname);

extern void TaskUpdate(Oid jobid, const char *schedule,
					   const char *command, const char *database,
					   const char *username, bool *active);

extern Oid GetTaskJobId(const char *jobname, const char *username);

extern char * GetTaskNameById(Oid jobid);

#endif			/* PG_TASK_H */
