
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/sysattr.h"
#include "catalog/indexing.h"
#include "catalog/pg_attrdef.h"
#include "catalog/pg_type.h"
#include "commands/matview.h"
#include "commands/trigger.h"

#include "replication/delta.h"

#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/json.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_lsn.h"
#include "utils/rel.h"
#include "utils/syscache.h"



PG_MODULE_MAGIC;

extern void		_PG_init(void);
extern void	PGDLLEXPORT	_PG_delta_plugin_init(DeltaTableCallbacks *cb);

/* These must be available to pg_dlsym() */
static void pg_delta_startup(DeltaOutputCtx *ctx, Relation rel, int serial_id);
static void pg_delta_shutdown(DeltaOutputCtx *ctx);
static DeltaOutputState* pg_delta_change(DeltaOutputCtx *ctx,
				 			Relation rel);
static int64 pg_delta_get_latest_snapshot_id(void *ctx);
static int64 pg_delta_successor_of(int64 snapshotId, void *ctx);
static int64 pg_delta_ancestors_of(int64 snapshotId, void *ctx);
static DeltaVersion pg_delta_get_version(int64 snapshotId, void *ctx);

static DeltaVersion VersionNone = { .snapshot_id = -1, .timestamp = 0, .event = 0 };


void
_PG_init(void)
{
}

/* Specify output plugin callbacks */
void
_PG_delta_plugin_init(DeltaTableCallbacks *cb)
{
	AssertVariableIsOfType(&_PG_delta_plugin_init, DeltaOutputPluginInit);

	cb->startup_cb = pg_delta_startup;
	cb->change_cb = pg_delta_change;
	cb->shutdown_cb = pg_delta_shutdown;
	cb->latest_snapshot_cb = pg_delta_get_latest_snapshot_id;
	cb->successor_of_cb = pg_delta_successor_of;
	cb->ancestors_of_cb = pg_delta_ancestors_of;
	cb->get_version_cb = pg_delta_get_version;
}

static void
pg_delta_startup(DeltaOutputCtx *ctx, Relation rel, int serial_id)
{
	DeltaOutputState *p = palloc0(sizeof(DeltaOutputState));

	p->serial_id = serial_id;
	p->relid = RelationGetRelid(rel);
	p->returned_rows = 0;
	p->tupdesc = RelationGetDescr(rel);
	ctx->output_writer_private = p;
}

static void
pg_delta_shutdown(DeltaOutputCtx *ctx)
{
	DeltaOutputState *p = (DeltaOutputState*) ctx->output_writer_private;

	if (!p)
		return;

	if (p->tg_newtable)
	{
		tuplestore_clear(p->tg_newtable);
		p->tg_newtable = NULL;
	}
	if (p->tg_oldtable)
	{
		tuplestore_clear(p->tg_oldtable);
		p->tg_oldtable = NULL;
	}
}

static DeltaOutputState*
pg_delta_change(DeltaOutputCtx *ctx,
				Relation rel)
{
	DeltaOutputState *p = (DeltaOutputState*) ctx->output_writer_private;
	HeapTuple tuple;
	TupleDesc	tupdesc;
	Datum	   *values;
	bool	   *nulls;

	p->tg_newtable = tuplestore_begin_heap(true, false, work_mem);

	char *name = make_delta_enr_name("new", p->relid, p->serial_id);

	p->event = TRIGGER_EVENT_INSERT;

	tuplestore_set_sharedname(p->tg_newtable, name);
	tuplestore_set_tableid(p->tg_newtable, RelationGetRelid(rel));

	tupdesc = p->tupdesc;
	values = (Datum *) palloc(tupdesc->natts * sizeof(Datum));
	nulls = (bool *) palloc(tupdesc->natts * sizeof(bool));

	for (int i = 0; i < tupdesc->natts; i++)
	{
		values[i] = random();
		nulls[i] = false;
	}

	tuple = heap_form_tuple(tupdesc, values, nulls);

	tuplestore_puttuple(p->tg_newtable, tuple);

	p->returned_rows++;

	p->error_code = 0;

	return p;
}


static int64
pg_delta_get_latest_snapshot_id(void *ctx)
{
	return -1;
}

static int64
pg_delta_successor_of(int64 snapshotId, void *ctx)
{
	return -1;
}

static int64
pg_delta_ancestors_of(int64 snapshotId, void *ctx)
{
	return -1;
}

static DeltaVersion
pg_delta_get_version(int64 snapshotId, void *ctx)
{
	return VersionNone;
}