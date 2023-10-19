/*-------------------------------------------------------------------------
 *
 * inval.h
 *	  POSTGRES cache invalidation dispatcher definitions.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/inval.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef INVAL_H
#define INVAL_H

#include "access/htup.h"
#include "storage/relfilenode.h"
#include "storage/sinval.h"
#include "utils/relcache.h"

/*
 * To minimize palloc traffic, we keep pending requests in successively-
 * larger chunks (a slightly more sophisticated version of an expansible
 * array).  All request types can be stored as SharedInvalidationMessage
 * records.  The ordering of requests within a list is never significant.
 */
typedef struct InvalidationChunk
{
    struct InvalidationChunk *next; /* list link */
    int			nitems;			/* # items currently stored in chunk */
    int			maxitems;		/* size of allocated array in this chunk */
    SharedInvalidationMessage msgs[FLEXIBLE_ARRAY_MEMBER];
} InvalidationChunk;

typedef struct InvalidationListHeader
{
    InvalidationChunk *cclist;	/* list of chunks holding catcache msgs */
    InvalidationChunk *rclist;	/* list of chunks holding relcache msgs */
} InvalidationListHeader;

typedef struct CacheAsyncMessages
{
    List *local_inval_messages;
    bool reset_cache_state;
} CacheAsyncMessages;

extern PGDLLIMPORT int debug_discard_caches;
extern CacheAsyncMessages *cache_async_messages;

typedef void (*SyscacheCallbackFunction) (Datum arg, int cacheid, uint32 hashvalue);
typedef void (*RelcacheCallbackFunction) (Datum arg, Oid relid);


extern void AcceptInvalidationMessages(void);

extern void AtEOXact_Inval(bool isCommit);

extern void AtEOSubXact_Inval(bool isCommit);

extern void PostPrepare_Inval(void);

extern void CommandEndInvalidationMessages(void);

extern void CacheInvalidateHeapTuple(Relation relation,
									 HeapTuple tuple,
									 HeapTuple newtuple);

extern void CacheInvalidateCatalog(Oid catalogId);

extern void CacheInvalidateRelcache(Relation relation);

extern void CacheInvalidateRelcacheAll(void);

extern void CacheInvalidateRelcacheByTuple(HeapTuple classTuple);

extern void CacheInvalidateRelcacheByRelid(Oid relid);

extern void CacheInvalidateSmgr(RelFileNodeBackend rnode);

extern void CacheInvalidateRelmap(Oid databaseId);

extern void CacheRegisterSyscacheCallback(int cacheid,
										  SyscacheCallbackFunction func,
										  Datum arg);

extern void CacheRegisterRelcacheCallback(RelcacheCallbackFunction func,
										  Datum arg);

extern void CallSyscacheCallbacks(int cacheid, uint32 hashvalue);

extern void InvalidateSystemCaches(void);
extern void InvalidateSystemCachesExtended(bool debug_discard);

extern void LogLogicalInvalidations(void);

typedef void (*CollectInvalMessages_hook_type) (SharedInvalidationMessage *msg);
typedef void (*ProcessResetCache_hook_type) (void);
typedef void (*cache_invalidation_async_hook_type) (CacheAsyncMessages *cache_async_messages);
typedef void (*cache_async_cleanup_hook_type) (CacheAsyncMessages *cache_async_messages);
extern PGDLLIMPORT CollectInvalMessages_hook_type CollectInvalMessages_hook;
extern PGDLLIMPORT ProcessResetCache_hook_type ProcessResetCache_hook;
extern PGDLLIMPORT cache_invalidation_async_hook_type cache_invalidation_async_hook;
extern PGDLLIMPORT cache_async_cleanup_hook_type cache_async_cleanup_hook;
#endif							/* INVAL_H */
