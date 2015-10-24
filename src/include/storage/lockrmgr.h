/*-------------------------------------------------------------------------
 *
 * lockrmgr.h
 *
 * Lock subsystems two-phase state file hooks.
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOCKRMGR_H_
#define LOCKRMGR_H_

#include "access/xlog.h"

extern void lock_recover_record(TransactionId xid, void *data, uint32 len);
extern void lock_postcommit(TransactionId xid, void *dummy1, uint32 dummy2);
extern void lock_postabort(TransactionId xid, void *dummy1, uint32 dummy2);

#endif   /* LOCKRMGR_H */
