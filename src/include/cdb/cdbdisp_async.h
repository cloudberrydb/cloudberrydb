/*-------------------------------------------------------------------------
 *
 * cdbdisp_async.h
 * routines for asynchronous implementation of dispatching commands
 * to the qExec processes.
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	    src/include/cdb/cdbdisp_async.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBDISP_ASYNC_H
#define CDBDISP_ASYNC_H

extern DispatcherInternalFuncs DispatcherAsyncFuncs;

extern void *cdbdisp_makeDispatchParams_async(int maxSlices, int largestGangSize, char *queryText, int len);

extern bool cdbdisp_checkAckMessage_async(struct CdbDispatcherState *ds, const char *message,
					  int timeout_sec);

extern void cdbdisp_checkDispatchResult_async(struct CdbDispatcherState *ds,
					      DispatchWaitMode waitMode);

extern void cdbdisp_dispatchToGang_async(struct CdbDispatcherState *ds,
					 struct Gang *gp,
					 int sliceIndex);
extern void	cdbdisp_waitDispatchFinish_async(struct CdbDispatcherState *ds);

extern bool	cdbdisp_checkForCancel_async(struct CdbDispatcherState *ds);
extern int *cdbdisp_getWaitSocketFds_async(struct CdbDispatcherState *ds, int *nsocks);
#endif
