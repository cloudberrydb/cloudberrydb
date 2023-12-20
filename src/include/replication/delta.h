#ifndef _DELTA_H
#define _DELTA_H

#include "access/xlog.h"
#include "access/xlogreader.h"
#include "replication/slot.h"
#include "c.h"

struct DeltaOutputCtx;
struct DeltaTableCallbacks;

typedef struct DeltaVersion
{
	int64 snapshot_id; /* snapshot uuid */
	TimestampTz timestamp; /* event happend timestamp */
	int32 event; /* reuse TRIGGER_EVENT_XXX bits*/
} DeltaVersion;

/*
 * Type of the shared library symbol _PG_delta_plugin_init that is looked up
 * when loading an output plugin shared library.
 */
typedef void (*DeltaOutputPluginInit) (struct DeltaTableCallbacks *cb);

/*
 * Callback that gets called in a user-defined plugin. ctx->private_data can
 * be set to some private data.
 *
 * "is_init" will be set to "true" if the decoding slot just got defined. When
 * the same slot is used from there one, it will be "false".
 */
typedef void (*DeltaOutputStartupCB) (struct DeltaOutputCtx *ctx,
									  List *options,
									  bool is_init);

/*
 * Callback for every individual change in a successful transaction.
 */
typedef void (*DeltaOutputChangeCB) (struct DeltaOutputCtx *ctx,
									   Relation relation,
									   ReorderBufferChange *change);


typedef void (*DeltaOutputShutdownCB) (struct DeltaOutputCtx *ctx);

typedef int64 (*DeltaOutputGetLatestSnapshotIdCB) (void *ctx);

typedef int64 (*DeltaOutputSuccessorOfCB) (int64 snapshotId, void *ctx);

typedef DeltaVersion (*DeltaOutputGetVersionCB) (int64 snapshotId, void *ctx);

typedef struct DeltaTableCallbacks
{
	DeltaOutputStartupCB startup_cb;
	DeltaOutputChangeCB change_cb;
	DeltaOutputShutdownCB shutdown_cb;
	DeltaOutputGetLatestSnapshotIdCB latest_snapshot_cb;
	DeltaOutputSuccessorOfCB successor_of_cb;
	DeltaOutputGetVersionCB get_version_cb;
} DeltaTableCallbacks;


typedef struct DeltaOutputCtx
{
	/* memory context this is all allocated in */
	MemoryContext context;

	Oid		    tableid;
	/*
	 * Marks the logical decoding context as fast forward decoding one.
	 */
	bool		fast_forward;

	/* Are we processing the end LSN of a transaction? */
	bool		end_xact;

	DeltaTableCallbacks callbacks;

	/*
	 * Private data pointer of the output plugin.
	 */
	void	   *output_plugin_private;

	/*
	 * Private data pointer for the data writer.
	 */
	void	   *output_writer_private;

	/*
	 * State for writing output.
	 */
	int64	    cur_snapshot_id;
	int64       prev_snapshot_id;
	TransactionId write_xid;
} DeltaOutputCtx;


extern DeltaOutputCtx *CreateDeltaContext(const char* plugin, int64 start_id, bool fast_forward);
extern bool DeltaPluginIsReady(const char* plugin);
extern void FreeDeltaContext(DeltaOutputCtx *ctx);


#endif