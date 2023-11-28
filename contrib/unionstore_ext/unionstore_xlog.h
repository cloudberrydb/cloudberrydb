#ifndef UNIONSTORE_XLOG_H
#define UNIONSTORE_XLOG_H

#include "access/xlog_internal.h"

/*
 * Pseudo block number used to associate LSN with relation metadata (relation size)
 */
#define REL_METADATA_PSEUDO_BLOCKNO InvalidBlockNumber

#define MAX_WAL_WRITE_SIZE (XLOG_BLCKSZ * 32)

extern XLogRecPtr UnionStoreXactLastRecEnd;
extern int lastWrittenLsnCacheSize;

extern Size UnionStoreXLOGShmemSize(void);
extern void UnionStoreXLOGShmemInit(void);
extern uint64 GetZenithCurrentClusterSize(void);
extern void SetZenithCurrentClusterSize(uint64 size);
extern XLogRecPtr GetLastWrittenLSN(RelFileNode relfilenode, ForkNumber forknum, BlockNumber blkno);
extern void SetLastWrittenLSNForBlockRange(XLogRecPtr lsn, RelFileNode rnode, ForkNumber forknum, BlockNumber from, BlockNumber n_blocks);
extern void SetLastWrittenLSNForBlock(XLogRecPtr lsn, RelFileNode rnode, ForkNumber forknum, BlockNumber blkno);
extern void SetLastWrittenLSNForRelation(XLogRecPtr lsn, RelFileNode rnode, ForkNumber forknum);
extern void SetUnionStoreFlushedLSN(XLogRecPtr remote_lsn, XLogRecPtr local_lsn);
extern XLogRecPtr UnionStoreXLogInsertRecord(XLogRecData *rdata, bool calculate_crc);
extern XLogRecPtr GetUnionStoreInsertRecPtr(void);
extern XLogRecPtr GetUnionStoreLocalFlushLsn(void);
extern XLogRecPtr GetUnionStoreRemoteFlushLsn(void);
extern bool GetUnionStoreWalRecords(char *buf, XLogRecPtr *start_pos, XLogRecPtr *end_pos, bool sentAnything);
extern void AdvanceWalCounter(void);
extern void *GetUnionStoreXLogCtlLatch(void);
extern void *GetUnionStoreStartupLatch(void);
extern void UnionStoreXLogFlush(XLogRecPtr RecPtr);
extern void UnionStoreInsertRecord(RmgrId rmid, uint8 info, XLogRecData *rdata);
extern XLogRecPtr UnionStoreXLogInsert(RmgrId rmid, uint8 info, TransactionId headerXid, uint8 curinsert_flags, RecordAssembleFunc recordAssembleFunc);
extern int WakeupUnionStoreWaiters(XLogRecPtr RecPtr);
extern void UnionStoreSetAbortLocalLsn(XLogRecPtr localAbortedLsn);
#endif          /* UNIONSTORE_XLOG_H */