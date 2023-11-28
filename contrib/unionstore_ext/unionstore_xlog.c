/*-------------------------------------------------------------------------
 *
 * unionstore_xlog.c
 *      Union Store write-ahead log manager
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"


#include "access/bitmap_xlog.h"
#include "access/gistxlog.h"
#include "access/heapam_xlog.h"
#include "access/nbtxlog.h"
#include "access/xact.h"
#include "access/xloginsert.h"
#include "catalog/storage_xlog.h"
#include "cdb/cdbvars.h"
#include "commands/dbcommands_xlog.h"
#include "pagestore_client.h"
#include "pgstat.h"
#include "replication/syncrep.h"
#include "replication/walsender_private.h"
#include "storage/buf_internals.h"
#include "storage/checksum.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "unionstore_xlog.h"
#include "utils/ps_status.h"



#define WAL_COUNTER_SIZE (1024)
#define WAL_BUFFER_SIZE (16777216)				/* 16MB -- resident in the L3 cache */

/*
 * batch WAL records info, used for WAL writing.
 *
 * The lsn(local) of batch records must be continuous.
 */
typedef struct WalCounter
{
    /*
	 * num of active writers in this batch, 0 means all writers
	 * have finished their writes.
	 */
    pg_atomic_uint32	count;

    /*
	 * start offset in wal buffer of this batch records.
	 */
    int64				offset;

    /*
	 * total WAL size of this batch records.
	 */
    int64				totalWalSize;

    /*
	 * start/end local lsn of this batch records.
	 */
    uint64				startLocalLsn;
    uint64				endLocalLsn;
} WalCounter;

/*
 * Shared memory state for Union Store XLOG
 *
 * Note: LSN in Union Store is "usable byte position", not XLogRecPtr.
 * A usable byte position is the position starting from the beginning of WAL, excluding all WAL
 * page headers.
 */
typedef struct UnionStoreXLogCtlData
{
    /* protect below members */
    slock_t				insert_lock;

    /*
	 * The next record will be inserted at this position in Union Store.
	 */
    uint64				CurrBytePos;

    /*
	 * ring buffer for union store's XLOG.
	 */
    char				*walBuffer; /* 16MB */
    int64				bufHead;
    int64				bufTail;
    int64				bufFreeSize;

    /*
	 * WalCounter for XLog Write.
	 */
    WalCounter			*counter;
    /*
	 * Current wal counter index.
	 * Backends write records into wal buffer, and update batch records info of current wal counter.
	 */
    int32				counterHead;
    /*
	 * Wal counter index used by WAL Proposer.
	 * WAL Proposer writes batch records according to the info of this wal counter.
	 */
    int32				counterTail;

    /*
	 * latest flushed LSN in union store, updated by WAL Proposer.
	 */
    pg_atomic_uint64	remoteFlushedLsn;

    /*
	 * local flushed lsn pairs with remote flushed lsn,
	 * updated by WAL Proposer.
	 */
    pg_atomic_uint64	localFlushedLsn;

    /*
     * any open transactions write wal before this local lsn should be aborted.
     */
    pg_atomic_uint64    localAbortedLsn;

    /*
	 * wakeupLatch is used to wake up the WAL Proposer to write
	 * WAL into Union Store.
	 */
    Latch				wakeupLatch;

    /*
     * startupLatch is used to wake up the WAL Proposer to do the startup works.
     */
    Latch               startupLatch;

    /*
	 * Maximal last written LSN for pages not present in lastWrittenLsnCache
	 */
    XLogRecPtr 			maxLastWrittenLsn;

    /*
	 * Double linked list to implement LRU replacement policy for last written LSN cache.
	 * Access to this list as well as to last written LSN cache is protected by 'LastWrittenLsnLock'.
	 */
    dlist_head			lastWrittenLsnLRU;

    /*
	 * size of a timeline in zenith pageserver.
	 * used to enforce timeline size limit.
	 */
    pg_atomic_uint64	zenithCurrentClusterSize;

    /*
     * Synchronous UnionStore queue used to wait XLOG flushed in UnionStore.
     * Protected by SyncRepLock.
     */
    SHM_QUEUE	SyncUnionStoreQueue;
} UnionStoreXLogCtlData;

static UnionStoreXLogCtlData *UnionStoreXLogCtl = NULL;

/*
 * Same as XactLastRecEnd, but points to
 * end+1 of the last record wrote in Union Store.
 */
XLogRecPtr	UnionStoreXactLastRecEnd = InvalidXLogRecPtr;

/*
 * Points to start of the first record of current transaction wrote in Union Store.
 */
XLogRecPtr	UnionStoreXactFirstRecPtr = InvalidXLogRecPtr;

#define SYNC_REP_CHECK_ABORT		3

#define AbortCurrentTransaction()                                                               \
do {                                                                                            \
    if (!XLogRecPtrIsInvalid(UnionStoreXactFirstRecPtr) &&                                      \
        UnionStoreXactFirstRecPtr < pg_atomic_read_u64(&UnionStoreXLogCtl->localAbortedLsn))    \
    {                                                                                           \
        UnionStoreXactFirstRecPtr = InvalidXLogRecPtr;                                          \
        /* We need to abort current transaction. */                                             \
        elog(ERROR, "Abort current transaction due to exception of wal service.");              \
    }                                                                                           \
} while (0)

typedef struct LastWrittenLsnCacheEntry
{
    BufferTag	key;
    XLogRecPtr	lsn;
    /* double linked list for LRU replacement algorithm */
    dlist_node	lru_node;
} LastWrittenLsnCacheEntry;

int	lastWrittenLsnCacheSize = 131072;

/*
 * Cache of last written LSN for each relation page.
 * Also to provide request LSN for smgrnblocks, smgrexists there is pseudokey=InvalidBlockId which stores LSN of last
 * relation metadata update.
 * Size of the cache is limited by GUC variable lastWrittenLsnCacheSize ("lsn_cache_size"),
 * pages are replaced using LRU algorithm, based on L2-list.
 * Access to this cache is protected by 'LastWrittenLsnLock'.
 */
static HTAB *lastWrittenLsnCache;

static LWLockId LastWrittenLsnLock;

static void WaitForUnionStoreFlush(XLogRecPtr lsn);
static void SyncUnionStoreQueueInsert(void);
static void SyncUnionStoreCancelWait(void);

static Size
UnionStoreXLogCtlSize(void)
{
    Size 	size;

    /* union store XLogCtl */
    size = sizeof(UnionStoreXLogCtlData);
    /* add wal buffer size */
    size = add_size(size, WAL_BUFFER_SIZE);
    /* add wal counter size */
    size = add_size(size, mul_size(sizeof(WalCounter), WAL_COUNTER_SIZE));

    return size;
}

/*
 * Calculate the union store's XLogCtlData size/written LSN cache size
 */
Size
UnionStoreXLOGShmemSize(void)
{
    Size 	size = UnionStoreXLogCtlSize();

    if (lastWrittenLsnCacheSize > 0)
        size = add_size(size, hash_estimate_size(lastWrittenLsnCacheSize, sizeof(LastWrittenLsnCacheEntry)));

    return size;
}

void
UnionStoreXLOGShmemInit(void)
{
    bool	found;

    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

    if (lastWrittenLsnCacheSize > 0)
    {
        static HASHCTL info;
        info.keysize = sizeof(BufferTag);
        info.entrysize = sizeof(LastWrittenLsnCacheEntry);
        lastWrittenLsnCache = ShmemInitHash("last_written_lsn_cache",
                                            lastWrittenLsnCacheSize, lastWrittenLsnCacheSize,
                                            &info,
                                            HASH_ELEM | HASH_BLOBS);
    }

    LastWrittenLsnLock = (LWLockId)GetNamedLWLockTranche("LastWrittenLsnLock");

    /* initialization of Union Store XLogCtl shared data */
    UnionStoreXLogCtl = (UnionStoreXLogCtlData *)
            ShmemInitStruct("Union Store XLOG Ctl", UnionStoreXLogCtlSize(), &found);
    if (!found)
    {
        Size offset = sizeof(UnionStoreXLogCtlData);

        memset(UnionStoreXLogCtl, 0, sizeof(UnionStoreXLogCtlData));

        SpinLockInit(&UnionStoreXLogCtl->insert_lock);

        /*
         * CurrBytePos
         * remoteFlushedLsn
         * localFlushedLsn
         * maxLastWrittenLsn
         *
         * inited by WAL Proposer.
         */
        UnionStoreXLogCtl->bufFreeSize = WAL_BUFFER_SIZE;
        UnionStoreXLogCtl->bufHead = 0;
        UnionStoreXLogCtl->bufTail = 0;
        UnionStoreXLogCtl->walBuffer = (char *)UnionStoreXLogCtl + offset;
        offset += WAL_BUFFER_SIZE;

        UnionStoreXLogCtl->counterHead = 0;
        UnionStoreXLogCtl->counterTail = 0;
        pg_atomic_init_u64(&UnionStoreXLogCtl->remoteFlushedLsn, 0);
        pg_atomic_init_u64(&UnionStoreXLogCtl->localFlushedLsn, 0);
        pg_atomic_init_u64(&UnionStoreXLogCtl->localAbortedLsn, 0);
        UnionStoreXLogCtl->counter = (WalCounter *)((char *)UnionStoreXLogCtl + offset);
        for (int index = 0; index < WAL_COUNTER_SIZE; index++)
        {
            pg_atomic_init_u32(&UnionStoreXLogCtl->counter[index].count, 0);
            UnionStoreXLogCtl->counter[index].offset = -1;
            UnionStoreXLogCtl->counter[index].totalWalSize = 0;
            UnionStoreXLogCtl->counter[index].startLocalLsn = InvalidXLogRecPtr;
            UnionStoreXLogCtl->counter[index].endLocalLsn = InvalidXLogRecPtr;
        }
        InitSharedLatch(&UnionStoreXLogCtl->wakeupLatch);
        InitSharedLatch(&UnionStoreXLogCtl->startupLatch);
        dlist_init(&UnionStoreXLogCtl->lastWrittenLsnLRU);
        pg_atomic_init_u64(&UnionStoreXLogCtl->zenithCurrentClusterSize, 0);
        SHMQueueInit(&(UnionStoreXLogCtl->SyncUnionStoreQueue));
    }

    LWLockRelease(AddinShmemInitLock);
}

/*
 * Return tenant size on PageServer, used to enforce tenant size limit.
 */
uint64
GetZenithCurrentClusterSize(void)
{
    return pg_atomic_read_u64(&UnionStoreXLogCtl->zenithCurrentClusterSize);
}

/*
 * Update tenant size, called by WAL Proposer.
 */
void
SetZenithCurrentClusterSize(uint64 size)
{
    pg_atomic_write_u64(&UnionStoreXLogCtl->zenithCurrentClusterSize, size);
}

/*
 * GetLastWrittenLSN -- Returns maximal LSN of written page.
 * It returns an upper bound for the last written LSN of a given page,
 * either from a cached last written LSN or a global maximum last written LSN.
 * If rnode is InvalidOid then we calculate maximum among all cached LSN and maxLastWrittenLsn.
 * If cache is large enough, iterating through all hash items may be rather expensive.
 * But GetLastWrittenLSN(InvalidOid) is used only by zenith_dbsize which is not performance critical.
 */
XLogRecPtr
GetLastWrittenLSN(RelFileNode rnode, ForkNumber forknum, BlockNumber blkno)
{
    XLogRecPtr lsn;
    LastWrittenLsnCacheEntry* entry;

    LWLockAcquire(LastWrittenLsnLock, LW_SHARED);

    /* Maximal last written LSN among all non-cached pages */
    lsn = UnionStoreXLogCtl->maxLastWrittenLsn;

    if (rnode.relNode != InvalidOid)
    {
        BufferTag key;
        key.rnode = rnode;
        key.forkNum = forknum;
        key.blockNum = blkno;
        entry = hash_search(lastWrittenLsnCache, &key, HASH_FIND, NULL);
        if (entry != NULL)
            lsn = entry->lsn;
    }
    else
    {
        HASH_SEQ_STATUS seq;
        /* Find maximum of all cached LSNs */
        hash_seq_init(&seq, lastWrittenLsnCache);
        while ((entry = (LastWrittenLsnCacheEntry *) hash_seq_search(&seq)) != NULL)
        {
            if (entry->lsn > lsn)
                lsn = entry->lsn;
        }
    }
    LWLockRelease(LastWrittenLsnLock);

    return lsn;
}

/*
 * SetLastWrittenLSNForBlockRange -- Set maximal LSN of written page range.
 * We maintain cache of last written LSNs with limited size and LRU replacement
 * policy. Keeping last written LSN for each page allows to use old LSN when
 * requesting pages of unchanged or appended relations. Also it is critical for
 * efficient work of prefetch in case massive update operations (like vacuum or remove).
 *
 * rnode.relNode can be InvalidOid, in this case maxLastWrittenLsn is updated.
 * SetLastWrittenLsn with dummy rnode is used by createdb and dbase_redo functions.
 */
void
SetLastWrittenLSNForBlockRange(XLogRecPtr lsn, RelFileNode rnode, ForkNumber forknum, BlockNumber from, BlockNumber n_blocks)
{
    if (lsn == InvalidXLogRecPtr || n_blocks == 0 || lastWrittenLsnCacheSize == 0)
        return;

    LWLockAcquire(LastWrittenLsnLock, LW_EXCLUSIVE);
    if (rnode.relNode == InvalidOid)
    {
        if (lsn > UnionStoreXLogCtl->maxLastWrittenLsn)
            UnionStoreXLogCtl->maxLastWrittenLsn = lsn;
    }
    else
    {
        LastWrittenLsnCacheEntry* entry;
        BufferTag key;
        bool found;
        BlockNumber i;

        key.rnode = rnode;
        key.forkNum = forknum;
        for (i = 0; i < n_blocks; i++)
        {
            key.blockNum = from + i;
            entry = hash_search(lastWrittenLsnCache, &key, HASH_ENTER, &found);
            if (found)
            {
                if (lsn > entry->lsn)
                    entry->lsn = lsn;
                /* Unlink from LRU list */
                dlist_delete(&entry->lru_node);
            }
            else
            {
                entry->lsn = lsn;
                if (hash_get_num_entries(lastWrittenLsnCache) > lastWrittenLsnCacheSize)
                {
                    /* Replace least recently used entry */
                    LastWrittenLsnCacheEntry* victim = dlist_container(LastWrittenLsnCacheEntry, lru_node, dlist_pop_head_node(&UnionStoreXLogCtl->lastWrittenLsnLRU));
                    /* Adjust max LSN for not cached relations/chunks if needed */
                    if (victim->lsn > UnionStoreXLogCtl->maxLastWrittenLsn)
                        UnionStoreXLogCtl->maxLastWrittenLsn = victim->lsn;

                    hash_search(lastWrittenLsnCache, victim, HASH_REMOVE, NULL);
                }
            }
            /* Link to the end of LRU list */
            dlist_push_tail(&UnionStoreXLogCtl->lastWrittenLsnLRU, &entry->lru_node);
        }
    }
    LWLockRelease(LastWrittenLsnLock);
}

/*
 * SetLastWrittenLSNForBlock -- Set maximal LSN for block
 */
void
SetLastWrittenLSNForBlock(XLogRecPtr lsn, RelFileNode rnode, ForkNumber forknum, BlockNumber blkno)
{
    SetLastWrittenLSNForBlockRange(lsn, rnode, forknum, blkno, 1);
}

/*
 * SetLastWrittenLSNForRelation -- Set maximal LSN for relation metadata
 */
void
SetLastWrittenLSNForRelation(XLogRecPtr lsn, RelFileNode rnode, ForkNumber forknum)
{
    SetLastWrittenLSNForBlock(lsn, rnode, forknum, REL_METADATA_PSEUDO_BLOCKNO);
}

/*
 * Set local/remote flushed LSN of Union Store.
 */
void
SetUnionStoreFlushedLSN(XLogRecPtr remote_lsn, XLogRecPtr local_lsn)
{
    XLogRecPtr localFlushedLsn = InvalidXLogRecPtr;

    Assert(!XLogRecPtrIsInvalid(remote_lsn));

    if (UnionStoreXLogCtl->CurrBytePos == 0)
    {
        /*
		 * At startup, we set maxLastWrittenLsn/local flushed lsn and insert pos to remote lsn.
		 */
        UnionStoreXLogCtl->CurrBytePos = remote_lsn;
        UnionStoreXLogCtl->maxLastWrittenLsn = remote_lsn;
        pg_atomic_write_u64(&UnionStoreXLogCtl->localFlushedLsn, remote_lsn);
        pg_atomic_write_u64(&UnionStoreXLogCtl->remoteFlushedLsn, remote_lsn);
        localFlushedLsn = remote_lsn;
    }
    else
    {
        pg_atomic_write_u64(&UnionStoreXLogCtl->remoteFlushedLsn, remote_lsn);
        if (!XLogRecPtrIsInvalid(local_lsn))
        {
            pg_atomic_write_u64(&UnionStoreXLogCtl->localFlushedLsn, local_lsn);
            localFlushedLsn = local_lsn;
        }
    }

    /*
     * Wake up any waiters whose local_lsn is smaller than localFlushedLsn.
     *
     * TODO: launch new thread/worker to do this job.
     */
    if (!XLogRecPtrIsInvalid(localFlushedLsn))
    {
        WakeupUnionStoreWaiters(localFlushedLsn);
    }
}

/*
 * Insert an XLOG record into Union Store's wal buffer, and the records in wal buffer
 * will be written into Union Store by WAL Proposer.
 */
XLogRecPtr
UnionStoreXLogInsertRecord(XLogRecData *rdata, bool calculate_crc)
{
    bool 		wait_wal_flush = false;
    uint64		startbytepos;
    uint64		endbytepos;
    uint64		offset;
    XLogRecPtr  wait_lsn;
    pg_crc32c	rdata_crc;
    WalCounter *curCounter;
    int32       counterHead;
    char 	   *walBuffer = UnionStoreXLogCtl->walBuffer;
    XLogRecord *rechdr = (XLogRecord *) rdata->data;
    int			size = MAXALIGN(rechdr->xl_tot_len);


    if (Gp_role == GP_ROLE_DISPATCH)
    {
        /*
         * Dispatcher should not write any WAL records into UnionStore.
         *
         * should not happen.
         */
        elog(ERROR, "Dispatcher should not write WAL to UnionStore.");
    }

    if (size > MAX_WAL_WRITE_SIZE)
    {
    	elog(ERROR, "WAL Record size exceeds Max limitation");
    }

Retry:
    if (wait_wal_flush)
    {
        UnionStoreXLogFlush(wait_lsn);
    }

    SpinLockAcquire(&UnionStoreXLogCtl->insert_lock);
    startbytepos = UnionStoreXLogCtl->CurrBytePos;
    if (startbytepos == 0)
    {
        /*
         * UnionStore's XLog system is not ready, wait for it.
         */
        SpinLockRelease(&UnionStoreXLogCtl->insert_lock);
        wait_wal_flush = true;
        wait_lsn = DEFAULT_XLOG_SEG_SIZE;
        goto Retry;
    }
    endbytepos = startbytepos + size;

    /* can fit in wal buffer */
    if (UnionStoreXLogCtl->bufFreeSize >= size)
    {
        counterHead = UnionStoreXLogCtl->counterHead;
        curCounter = &UnionStoreXLogCtl->counter[counterHead];

        /*
		 * current batch size exceeds the limits, advance to next batch.
		 */
        if (curCounter->totalWalSize + size > MAX_WAL_WRITE_SIZE)
        {
            counterHead = (UnionStoreXLogCtl->counterHead + 1) % WAL_COUNTER_SIZE;
            if (counterHead == UnionStoreXLogCtl->counterTail)
            {
                /* wait for WAL Proposer to write WAL records */
                SpinLockRelease(&UnionStoreXLogCtl->insert_lock);
                SetLatch(&UnionStoreXLogCtl->wakeupLatch);
                wait_wal_flush = true;
                wait_lsn = (startbytepos - WAL_BUFFER_SIZE + size);
                goto Retry;
            }
            UnionStoreXLogCtl->counterHead = counterHead;
            curCounter = &UnionStoreXLogCtl->counter[counterHead];
        }

        /*
		 * get offset for later write, then update wal counter's info.
		 */
        offset = UnionStoreXLogCtl->bufHead;
        UnionStoreXLogCtl->bufHead = (UnionStoreXLogCtl->bufHead + size) % WAL_BUFFER_SIZE;
        UnionStoreXLogCtl->bufFreeSize -= size;
        curCounter->totalWalSize += size;
        if (curCounter->offset == -1)
            curCounter->offset = offset;
        if (XLogRecPtrIsInvalid(curCounter->startLocalLsn))
            curCounter->startLocalLsn = startbytepos;
        curCounter->endLocalLsn = endbytepos;
        pg_atomic_fetch_add_u32(&curCounter->count, 1);
    }
    else
    {
        /*
		 * not enough space in buf, wake up WAL Proposer to write WAL records.
		 */
        SpinLockRelease(&UnionStoreXLogCtl->insert_lock);
        SetLatch(&UnionStoreXLogCtl->wakeupLatch);
        wait_wal_flush = true;
        wait_lsn = (startbytepos - WAL_BUFFER_SIZE + size);
        goto Retry;
    }

    UnionStoreXLogCtl->CurrBytePos = endbytepos;
    SpinLockRelease(&UnionStoreXLogCtl->insert_lock);

    if (calculate_crc)
    {
        /*
         * We do not know the exact prev lsn, set to invalid lsn instead.
         */
        rechdr->xl_prev = InvalidXLogRecPtr;

        /*
         * Now that xl_prev has been filled in, calculate CRC of the record
         * header.
         */
        rdata_crc = rechdr->xl_crc;
        COMP_CRC32C(rdata_crc, rechdr, offsetof(XLogRecord, xl_crc));
        FIN_CRC32C(rdata_crc);
        rechdr->xl_crc = rdata_crc;
    }

    /* write WAL record into buffer */
    while (rdata != NULL)
    {
        char	   *rdata_data = rdata->data;
        int			rdata_len = rdata->len;

        if (offset + rdata_len <= WAL_BUFFER_SIZE)
        {
            memcpy(walBuffer + offset, rdata_data, rdata_len);
            offset += rdata_len;
        }
        else
        {
            int write_len = WAL_BUFFER_SIZE - offset;
            memcpy(walBuffer + offset, rdata_data, write_len);
            offset = 0;
            memcpy(walBuffer + offset, rdata_data + write_len, rdata_len - write_len);
            offset += (rdata_len - write_len);
        }

        rdata = rdata->next;
    }

    /* memset the aligned part? */

    pg_atomic_fetch_sub_u32(&curCounter->count, 1);

    UnionStoreXactLastRecEnd = endbytepos;

    if (XLogRecPtrIsInvalid(UnionStoreXactFirstRecPtr))
    {
        UnionStoreXactFirstRecPtr = startbytepos;
    }

    MarkCurrentTransactionIdLoggedIfAny();

    return endbytepos;
}

/*
 * GetUnionStoreInsertRecPtr -- Returns the current local insert position of union store.
 */
XLogRecPtr
GetUnionStoreInsertRecPtr(void)
{
    XLogRecPtr	recptr;

    SpinLockAcquire(&UnionStoreXLogCtl->insert_lock);
    recptr = UnionStoreXLogCtl->CurrBytePos;
    SpinLockRelease(&UnionStoreXLogCtl->insert_lock);

    return recptr;
}

/*
 * GetUnionStoreLocalFlushLsn -- Returns the current local flush lsn, pairs with
 * remote flush lsn.
 */
XLogRecPtr
GetUnionStoreLocalFlushLsn(void)
{
    return pg_atomic_read_u64(&UnionStoreXLogCtl->localFlushedLsn);
}

/*
 * GetUnionStoreLocalFlushLsn -- Returns the current remote flush lsn in Union Store.
 */
XLogRecPtr
GetUnionStoreRemoteFlushLsn(void)
{
    return pg_atomic_read_u64(&UnionStoreXLogCtl->remoteFlushedLsn);
}

/*
 * Get batch records to write into Union Store, called by WAL Proposer.
 */
bool
GetUnionStoreWalRecords(char *buf, XLogRecPtr *start_pos, XLogRecPtr *end_pos, bool sentAnything)
{
Retry:
    if (UnionStoreXLogCtl->counterTail != UnionStoreXLogCtl->counterHead)
    {
        WalCounter *counter = &UnionStoreXLogCtl->counter[UnionStoreXLogCtl->counterTail];

        Assert(counter->startLocalLsn + counter->totalWalSize == counter->endLocalLsn);

        /*
		 * wait for all writes finished
		 */
        while (pg_atomic_read_u32(&counter->count) != 0)
        {
            /* polling here */
        }

        *start_pos = counter->startLocalLsn;
        *end_pos = counter->endLocalLsn;

        if (counter->offset + counter->totalWalSize <= WAL_BUFFER_SIZE)
            memcpy(buf, UnionStoreXLogCtl->walBuffer + counter->offset, counter->totalWalSize);
        else
        {
            int64 write_len = WAL_BUFFER_SIZE - counter->offset;
            memcpy(buf, UnionStoreXLogCtl->walBuffer + counter->offset, write_len);
            memcpy(buf + write_len, UnionStoreXLogCtl->walBuffer, counter->totalWalSize - write_len);
        }

        /* got it */
        return true;
    }
    else if (sentAnything)
    {
        /*
		 * send any records in wal buffer
		 */
        WalCounter *counter = &UnionStoreXLogCtl->counter[UnionStoreXLogCtl->counterTail];

        if (counter->totalWalSize > 0)
        {
            SpinLockAcquire(&UnionStoreXLogCtl->insert_lock);
            UnionStoreXLogCtl->counterHead = (UnionStoreXLogCtl->counterHead + 1) % WAL_COUNTER_SIZE;
            SpinLockRelease(&UnionStoreXLogCtl->insert_lock);
            goto Retry;
        }
    }

    /* no wal records to write */
    return false;
}

/*
 * After finished one batch records, return space to wal buffer and advacne wal counter tail.
 *
 * Before call this function, make sure we already successfully write batch records to Union Store.
 */
void
AdvanceWalCounter(void)
{
    int64	totalWalSize;
    WalCounter *counter = &UnionStoreXLogCtl->counter[UnionStoreXLogCtl->counterTail];

    Assert(UnionStoreXLogCtl->counterTail != UnionStoreXLogCtl->counterHead);
    Assert(pg_atomic_read_u32(&counter->count) == 0);

    counter->offset = -1;
    totalWalSize = counter->totalWalSize;
    counter->totalWalSize = 0;
    counter->startLocalLsn = 0;
    counter->endLocalLsn = 0;

    SpinLockAcquire(&UnionStoreXLogCtl->insert_lock);
    UnionStoreXLogCtl->counterTail = (UnionStoreXLogCtl->counterTail + 1) % WAL_COUNTER_SIZE;
    UnionStoreXLogCtl->bufTail = (UnionStoreXLogCtl->bufTail + totalWalSize) % WAL_BUFFER_SIZE;
    UnionStoreXLogCtl->bufFreeSize += totalWalSize;
    SpinLockRelease(&UnionStoreXLogCtl->insert_lock);

    Assert(UnionStoreXLogCtl->bufFreeSize <= WAL_BUFFER_SIZE);
}

void *
GetUnionStoreXLogCtlLatch(void)
{
    return &UnionStoreXLogCtl->wakeupLatch;
}

void *
GetUnionStoreStartupLatch(void)
{
    return &UnionStoreXLogCtl->startupLatch;
}

/*
 * Wait for the WAL flushed in Union Store.
 *
 * Like XLogFlush, but the RecPtr is local lsn of Union Store.
 */
void
UnionStoreXLogFlush(XLogRecPtr RecPtr)
{
    int32 old_CritSectionCount = CritSectionCount;

    if (RecPtr <= pg_atomic_read_u64(&UnionStoreXLogCtl->localFlushedLsn))
        return;

    /*
     * Should not panic here, we could abort transaction here.
     */
    CritSectionCount = 0;

    /*
     * Check whether we need to abort current transaction. Wal till localAbortedLsn may not
     * be persisted in wal service due to some exceptions. If we have sent wal before this lsn,
     * we have to abort transaction.
     *
     * TODO: fine-grained management.
     * We can record lsn ranges of all wals wrote in current transaction,
     * and if any wal's lsn range is overlapped with (localFlushedLsn, localAbortedLsn), abort the transaction.
     */
    AbortCurrentTransaction();

    /* wait for union store */
    WaitForUnionStoreFlush(RecPtr);

    /*
     * In race condition, check again!
     */
    AbortCurrentTransaction();

    CritSectionCount = old_CritSectionCount;

    Assert(RecPtr <= pg_atomic_read_u64(&UnionStoreXLogCtl->localFlushedLsn));
}

/*
 * WAL records of drop database/drop table should be written to
 * UnionStore, and UnionStore will drop storage according to these WAL.
 *
 * Write records satisfied those WAL types into UnionStore.
 */
void
UnionStoreInsertRecord(RmgrId rmid, uint8 info, XLogRecData *rdata)
{
    bool insert = false;
    bool is_xact_record = false;

    /*
     * Drop database
     *
     * Drop table/index -- commit/abort transaction WALs
     */
    switch (rmid)
    {
        case RM_XACT_ID:
        {
            uint8 xl_info = info & XLOG_XACT_OPMASK;

	            if (xl_info == XLOG_XACT_COMMIT || xl_info == XLOG_XACT_COMMIT_PREPARED ||
                xl_info == XLOG_XACT_DISTRIBUTED_COMMIT ||
                xl_info == XLOG_XACT_ABORT || xl_info == XLOG_XACT_ABORT_PREPARED)
            {
                insert = true;
                is_xact_record = true;
            }

            break;
        }
        case RM_DBASE_ID:
        {
            if ((info & ~XLR_INFO_MASK) == XLOG_DBASE_DROP)
            {
                insert = true;
            }
            break;
        }
        default:
            break;
    }

    if (insert)
    {
    	int32 old_CritSectionCount = CritSectionCount;
    	int32 old_InterruptHoldoffCount = InterruptHoldoffCount;
    	int32 old_QueryCancelHoldoffCount = QueryCancelHoldoffCount;

    	/*
    	 * Do not throw error.
    	 */
    	PG_TRY();
    	{
    		CritSectionCount = 0;

    		UnionStoreXLogInsertRecord(rdata, false);

    		CritSectionCount = old_CritSectionCount;
    	}
    	PG_CATCH();
    	{
    		CritSectionCount = old_CritSectionCount;
    		InterruptHoldoffCount = old_InterruptHoldoffCount;
    		QueryCancelHoldoffCount = old_QueryCancelHoldoffCount;
    		FlushErrorState();
    	}
    	PG_END_TRY();

        if (is_xact_record)
        {
            /*
             * Reset at the end of transaction.
             */
            UnionStoreXactLastRecEnd = InvalidXLogRecPtr;
            UnionStoreXactFirstRecPtr = InvalidXLogRecPtr;
        }
    }
}

XLogRecPtr
UnionStoreXLogInsert(RmgrId rmid, uint8 info, TransactionId headerXid, uint8 curinsert_flags, RecordAssembleFunc recordAssembleFunc)
{
    bool        UnionStoreInsert = false;
    bool        RegisterRnode_got = true;
    bool        SetLastWrittenLSN = false;
    bool        needPageWrites = false;
    XLogRecPtr	EndPos;
    RelFileNode rnode;
    ForkNumber forknum;
    BlockNumber blkno;

    rnode.relNode = 0;
    rnode.dbNode = 0;
    rnode.spcNode = 0;
    /*
     * We store table/index data related records into Union Store, including
     * Heap2/Heap/Btree/Hash/Gin/Gist/SPGist/BRIN/Bitmap.
     */
    switch (rmid)
    {
        case RM_HEAP_ID:
        {
            if (((info & ~XLR_INFO_MASK) & XLOG_HEAP_OPMASK) != XLOG_HEAP_TRUNCATE)
            {
                RegisterRnode_got = GetXLogRegisterBufferTagIfAny(&rnode, &forknum, &blkno);
            }
            break;
        }
        case RM_HEAP2_ID:
        {
            uint8 xl_info = ((info & ~XLR_INFO_MASK) & XLOG_HEAP_OPMASK);

            if (xl_info != XLOG_HEAP2_NEW_CID && xl_info != XLOG_HEAP2_REWRITE)
            {
                RegisterRnode_got = GetXLogRegisterBufferTagIfAny(&rnode, &forknum, &blkno);
            }
            break;
        }
        case RM_BTREE_ID:
        {
            if ((info & ~XLR_INFO_MASK) != XLOG_BTREE_REUSE_PAGE)
            {
                RegisterRnode_got = GetXLogRegisterBufferTagIfAny(&rnode, &forknum, &blkno);
            }
            break;
        }
        case RM_HASH_ID:
        case RM_GIN_ID:
        case RM_SPGIST_ID:
        case RM_BRIN_ID:
        {
            RegisterRnode_got = GetXLogRegisterBufferTagIfAny(&rnode, &forknum, &blkno);
            break;
        }
        case RM_BITMAP_ID:
        {
            if ((info & ~XLR_INFO_MASK) == XLOG_BITMAP_INSERT_META)
            {
                xl_bm_metapage *xlMeta = (xl_bm_metapage *)GetXLogRegisterRdata(0);

                rnode = xlMeta->bm_node;
            }
            else
            {
            	needPageWrites = true;
            	RegisterRnode_got = GetXLogRegisterBufferTagIfAny(&rnode, &forknum, &blkno);
            }
            break;
        }
        case RM_GIST_ID:
        {
            uint8 xl_info = info & ~XLR_INFO_MASK;

            if (xl_info != XLOG_GIST_PAGE_REUSE && xl_info != XLOG_GIST_ASSIGN_LSN)
            {
                RegisterRnode_got = GetXLogRegisterBufferTagIfAny(&rnode, &forknum, &blkno);
            }
            break;
        }
        case RM_SMGR_ID:
        {
            if ((info & ~XLR_INFO_MASK) == XLOG_SMGR_TRUNCATE)
            {
                xl_smgr_truncate *xlrec = (xl_smgr_truncate *)GetXLogRegisterRdata(0);

                rnode = xlrec->rnode;
            }
            else if ((info & ~XLR_INFO_MASK) == XLOG_SMGR_CREATE)
            {
                xl_smgr_create *xlrec = (xl_smgr_create *)GetXLogRegisterRdata(0);

                /*
                 * Set Last Written LSN for new created relation.
                 */
                SetLastWrittenLSN = true;
                rnode = xlrec->rnode;
                forknum = xlrec->forkNum;
            }
            break;
        }
        case RM_XLOG_ID:
        {
            uint8 xl_info = info & ~XLR_INFO_MASK;

            if (xl_info == XLOG_FPI || xl_info == XLOG_FPI_FOR_HINT)
            {
                RegisterRnode_got = GetXLogRegisterBufferTagIfAny(&rnode, &forknum, &blkno);
            }
            break;
        }
        case RM_XACT_ID:
        {
            uint8 xl_info = info & XLOG_XACT_OPMASK;

            if (xl_info == XLOG_XACT_COMMIT || xl_info == XLOG_XACT_COMMIT_PREPARED ||
                xl_info == XLOG_XACT_DISTRIBUTED_COMMIT || xl_info == XLOG_XACT_PREPARE)
            {
                /*
                 * Flush Union Store XLOG before writing Commit XLOG.
                 */
                UnionStoreXLogFlush(UnionStoreXactLastRecEnd);

                /*
                 * Reset at the end of transaction.
                 */
                UnionStoreXactLastRecEnd = InvalidXLogRecPtr;
                UnionStoreXactFirstRecPtr = InvalidXLogRecPtr;
            }
            else if (xl_info == XLOG_XACT_ABORT || xl_info == XLOG_XACT_ABORT_PREPARED)
            {
                /*
                 * Reset at the end of transaction.
                 */
                UnionStoreXactLastRecEnd = InvalidXLogRecPtr;
                UnionStoreXactFirstRecPtr = InvalidXLogRecPtr;
            }
            break;
        }
        default:
            break;
    }

    /* should not happen */
    if (!RegisterRnode_got)
    {
        elog(ERROR, "could not get register block relfilenode");
    }

    if (!RelFileNode_IsEmpty(&rnode))
    {
        SMgrRelation  reln = smgropen(rnode, InvalidBackendId, SMGR_MD, NULL);

        if (SmgrIsUnionStore(reln))
        {
            UnionStoreInsert = true;
        }
    }

    do
    {
        XLogRecPtr	RedoRecPtr;
        bool		doPageWrites;
        XLogRecPtr	fpw_lsn;
        XLogRecData *rdt;
        int			num_fpi = 0;

        if (UnionStoreInsert)
        {
            /*
			 * Do not need full Page Write except for bitmap index in Union Store.
			 */
            GetFullPageWriteInfo(&RedoRecPtr, &doPageWrites);

            rdt = (XLogRecData *) recordAssembleFunc(rmid, info, RedoRecPtr, needPageWrites,
                                     &fpw_lsn, headerXid, &num_fpi);

            EndPos = UnionStoreXLogInsertRecord(rdt, true);

            if (SetLastWrittenLSN)
            {
                SetLastWrittenLSNForRelation(EndPos, rnode, forknum);
            }
        }
        else
        {
            /*
             * Get values needed to decide whether to do full-page writes. Since
             * we don't yet have an insertion lock, these could change under us,
             * but XLogInsertRecord will recheck them once it has a lock.
             */
            GetFullPageWriteInfo(&RedoRecPtr, &doPageWrites);

            rdt = (XLogRecData *) recordAssembleFunc(rmid, info, RedoRecPtr, doPageWrites,
                                     &fpw_lsn, headerXid, &num_fpi);

            EndPos = XLogInsertRecord(rdt, fpw_lsn, curinsert_flags, num_fpi);

            /*
			 * Some WAL records need to be written into both local storage and UnionStore.
			 */
            UnionStoreInsertRecord(rmid, info, rdt);
        }
    } while (EndPos == InvalidXLogRecPtr);

    XLogResetInsertion();
	if (!UnionStoreInsert && EndPos != InvalidXLogRecPtr && (rmid == RM_DISTRIBUTEDLOG_ID || rmid == RM_CLOG_ID || rmid == RM_MULTIXACT_ID))
	{
		/*
		 * flush xlog before flush Union Store
		 */
		XLogFlush(EndPos);
	}

    return EndPos;
}

void
UnionStoreSetAbortLocalLsn(XLogRecPtr localAbortedLsn)
{
    pg_atomic_write_u64(&UnionStoreXLogCtl->localAbortedLsn, localAbortedLsn);
}

/*
 * Wait for Union Store flushed the WAL records.
 *
 * Copy of SyncRepWaitForLSN.
 */
static void
WaitForUnionStoreFlush(XLogRecPtr lsn)
{
    char	   *new_status = NULL;
    const char *old_status;

    Assert(SHMQueueIsDetached(&(MyProc->syncRepLinks)));

    LWLockAcquire(SyncRepLock, LW_EXCLUSIVE);
    Assert(MyProc->syncRepState == SYNC_REP_NOT_WAITING);

    if (lsn <= pg_atomic_read_u64(&UnionStoreXLogCtl->localFlushedLsn))
    {
        LWLockRelease(SyncRepLock);
        return;
    }

    /*
	 * Set our waitLSN so WALSender will know when to wake us, and add
	 * ourselves to the queue.
	 */
    MyProc->waitLSN = lsn;
    MyProc->syncRepState = SYNC_REP_WAITING;
    SyncUnionStoreQueueInsert();
    LWLockRelease(SyncRepLock);

    /* Alter ps display to show waiting for sync rep. */
    if (update_process_title)
    {
        int			len;

        old_status = get_real_act_ps_display(&len);
        /*
		 * The 32 represents the bytes in the string " waiting for %X/%X", as
		 * in upstream.  The 12 represents GPDB specific " replication" suffix.
		 */
        new_status = (char *) palloc(len + 32 + 12 + 1);
        memcpy(new_status, old_status, len);
        sprintf(new_status + len, " waiting for UnionStore %X/%X ",
                LSN_FORMAT_ARGS(lsn));
        set_ps_display(new_status);
        new_status[len] = '\0'; /* truncate off " waiting ..." */
    }

    /*
	 * Wait for specified LSN to be confirmed.
	 *
	 * Each proc has its own wait latch, so we perform a normal latch
	 * check/wait loop here.
	 */
    for (;;)
    {
        int			rc;

        /* Must reset the latch before testing state. */
        ResetLatch(MyLatch);

        /*
		 * Acquiring the lock is not needed, the latch ensures proper
		 * barriers. If it looks like we're done, we must really be done,
		 * because once walsender changes the state to SYNC_REP_WAIT_COMPLETE,
		 * it will never update it again, so we can't be seeing a stale value
		 * in that case.
		 */
        if (MyProc->syncRepState == SYNC_REP_WAIT_COMPLETE)
        {
            break;
        }
        else if (MyProc->syncRepState == SYNC_REP_CHECK_ABORT)
        {
            if (!XLogRecPtrIsInvalid(UnionStoreXactFirstRecPtr) &&
                UnionStoreXactFirstRecPtr < pg_atomic_read_u64(&UnionStoreXLogCtl->localAbortedLsn))
            {
                /* abort current transaction */
                SyncUnionStoreCancelWait();
                break;
            }
            MyProc->syncRepState = SYNC_REP_WAITING;
        }

       /*
        * Wait on latch.  Any condition that should wake us up will set the
        * latch, so no need for timeout.
        */
       rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_POSTMASTER_DEATH, -1,
                      WAIT_EVENT_SYNC_REP);

       if (ProcDiePending)
       {
           SyncUnionStoreCancelWait();
           ereport(ERROR,
                   (errcode(ERRCODE_ADMIN_SHUTDOWN),
                           errmsg("canceling the wait for UnionStore and terminating connection due to administrator command")));
       }

       if (QueryCancelPending)
       {
           QueryCancelPending = false;
           SyncUnionStoreCancelWait();
           ereport(ERROR,
                   (errmsg("canceling wait for UnionStore due to user request")));
       }

       /*
        * If the postmaster dies, we'll probably never get an acknowledgment,
        * because all the wal sender processes will exit. So just bail out.
        */
       if (rc & WL_POSTMASTER_DEATH)
       {
           ProcDiePending = true;
           SyncUnionStoreCancelWait();
           proc_exit(1);
           break;
       }
    }

    /*
	 * WalSender has checked our LSN and has removed us from queue. Clean up
	 * state and leave.  It's OK to reset these shared memory fields without
	 * holding SyncRepLock, because any walsenders will ignore us anyway when
	 * we're not on the queue.  We need a read barrier to make sure we see the
	 * changes to the queue link (this might be unnecessary without
	 * assertions, but better safe than sorry).
	 */
    pg_read_barrier();
    Assert(SHMQueueIsDetached(&(MyProc->syncRepLinks)));
    MyProc->syncRepState = SYNC_REP_NOT_WAITING;
    MyProc->waitLSN = 0;

    if (new_status)
    {
        /* Reset ps display */
        set_ps_display(new_status);
        pfree(new_status);
    }
}

/*
 * Copy of SyncRepQueueInsert.
 *
 * Use SyncUnionStoreQueue instead of SyncRepQueue.
 */
static void
SyncUnionStoreQueueInsert(void)
{
    PGPROC	   *proc;

    proc = (PGPROC *) SHMQueuePrev(&(UnionStoreXLogCtl->SyncUnionStoreQueue),
                                   &(UnionStoreXLogCtl->SyncUnionStoreQueue),
                                   offsetof(PGPROC, syncRepLinks));

    while (proc)
    {
        /*
		 * Stop at the queue element that we should after to ensure the queue
		 * is ordered by LSN.
		 */
        if (proc->waitLSN < MyProc->waitLSN)
            break;

        proc = (PGPROC *) SHMQueuePrev(&(UnionStoreXLogCtl->SyncUnionStoreQueue),
                                       &(proc->syncRepLinks),
                                       offsetof(PGPROC, syncRepLinks));
    }

    if (proc)
        SHMQueueInsertAfter(&(proc->syncRepLinks), &(MyProc->syncRepLinks));
    else
        SHMQueueInsertAfter(&(UnionStoreXLogCtl->SyncUnionStoreQueue), &(MyProc->syncRepLinks));
}

/*
 * Acquire SyncRepLock and cancel any wait currently in progress.
 */
static void
SyncUnionStoreCancelWait(void)
{
    LWLockAcquire(SyncRepLock, LW_EXCLUSIVE);
    if (!SHMQueueIsDetached(&(MyProc->syncRepLinks)))
        SHMQueueDelete(&(MyProc->syncRepLinks));
    MyProc->syncRepState = SYNC_REP_NOT_WAITING;
    LWLockRelease(SyncRepLock);
}

/*
 * Wake up waiters of Union Store.
 *
 * Copy of SyncRepWakeQueue.
 */
int
WakeupUnionStoreWaiters(XLogRecPtr RecPtr)
{
    PGPROC	   *proc = NULL;
    PGPROC	   *thisproc = NULL;
    int			numprocs = 0;

    LWLockAcquire(SyncRepLock, LW_EXCLUSIVE);

    proc = (PGPROC *) SHMQueueNext(&(UnionStoreXLogCtl->SyncUnionStoreQueue),
                                   &(UnionStoreXLogCtl->SyncUnionStoreQueue),
                                   offsetof(PGPROC, syncRepLinks));

    while (proc)
    {
        /*
		 * Assume the queue is ordered by LSN
		 */
        if (!XLogRecPtrIsInvalid(RecPtr) && RecPtr < proc->waitLSN)
            break;

        /*
		 * Move to next proc, so we can delete thisproc from the queue.
		 * thisproc is valid, proc may be NULL after this.
		 */
        thisproc = proc;
        proc = (PGPROC *) SHMQueueNext(&(UnionStoreXLogCtl->SyncUnionStoreQueue),
                                       &(proc->syncRepLinks),
                                       offsetof(PGPROC, syncRepLinks));

        /*
		 * Remove thisproc from queue.
		 */
        if (!XLogRecPtrIsInvalid(RecPtr))
            SHMQueueDelete(&(thisproc->syncRepLinks));

        /*
		 * SyncRepWaitForLSN() reads syncRepState without holding the lock, so
		 * make sure that it sees the queue link being removed before the
		 * syncRepState change.
		 */
        pg_write_barrier();

        /*
		 * Set state to complete; see SyncRepWaitForLSN() for discussion of
		 * the various states.
		 */
        if (!XLogRecPtrIsInvalid(RecPtr))
            thisproc->syncRepState = SYNC_REP_WAIT_COMPLETE;
        else
            thisproc->syncRepState = SYNC_REP_CHECK_ABORT;

        /*
		 * Wake only when we have set state and removed from queue.
		 */
        SetLatch(&(thisproc->procLatch));

        numprocs++;
    }

    LWLockRelease(SyncRepLock);

    return numprocs;
}