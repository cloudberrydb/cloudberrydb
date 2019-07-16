/*---------------------------------------------------------------------------
 *	Greenplum Database
 *	Copyright (c) 2004-2015 Pivotal Software, Inc.
 *
 *	@filename:
 *		_api.h
 *
 *	@doc:
 *		GPOS wrapper interface for GPDB.
 *
 *	@owner:
 *
 *	@test:
 *
 *
 *---------------------------------------------------------------------------*/
#ifndef GPOS_api_H
#define GPOS_api_H

#include "gpos/config.h"

#ifdef __cplusplus
extern "C"
{
#include <stddef.h>
#endif /* __cplusplus */

/*
 * struct with configuration parameters for task execution;
 * this needs to be in sync with the corresponding structure in optserver.h
 */
struct gpos_exec_params
{
	void *(*func)(void*);           /* task function */
	void *arg;                      /* task argument */
	void *result;                   /* task result */
	void *stack_start;              /* start of current thread's stack */
	char *error_buffer;             /* buffer used to store error messages */
	int error_buffer_size;          /* size of error message buffer */
	bool *abort_requested;          /* flag indicating if abort is requested */
};

/* struct containing initialization parameters for gpos */
struct gpos_init_params
{
	void *(*alloc)(size_t);			/* custom allocator */
	void (*free)(void*);			/* custom free      */
	bool (*abort_requested) (void);	/* callback to report abort requests */
};

/* initialize GPOS memory pool, worker pool and message repository */
void gpos_init(struct gpos_init_params *params);

/*
 * execute function as a GPOS task using current thread;
 * return 0 for successful completion, 1 for error
 */
int gpos_exec(gpos_exec_params *params);

/* shutdown GPOS memory pool, worker pool and message repository */
void gpos_terminate(void);

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* !GPOS_api_H */

// EOF

