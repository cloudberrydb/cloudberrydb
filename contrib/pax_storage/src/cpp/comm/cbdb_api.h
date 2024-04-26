#ifndef SRC_CPP_COMM_CBDB_API_H_
#define SRC_CPP_COMM_CBDB_API_H_

#include "comm/pax_rel.h"

#ifdef __cplusplus
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wregister"
extern "C" {
#endif

#include "postgres.h"  //  NOLINT
#include "postmaster/postmaster.h"

#include "access/detoast.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/relscan.h"
#include "access/sdir.h"
#include "access/tableam.h"
#include "access/tsmapi.h"
#include "access/tupdesc.h"
#include "access/tupdesc_details.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/gp_indexing.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/oid_dispatch.h"
#include "catalog/pg_am.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_amproc.h"
#include "catalog/pg_attribute_encoding.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_opclass.h"
#include "catalog/toasting.h"
#include "commands/progress.h"
#include "commands/tablecmds.h"
#include "nodes/execnodes.h"
#include "funcapi.h"
#include "partitioning/partdesc.h"
#include "partitioning/partbounds.h"
#include "pgstat.h"
#include "utils/partcache.h"
#include "utils/ruleutils.h"
#include "utils/varlena.h"
#include "access/nbtree.h"
#include "access/hash.h"
#include "parser/parse_utilcmd.h"
#include "nodes/makefuncs.h"
#include "nodes/bitmapset.h"
#include "parser/parse_oper.h"
#include "parser/parse_expr.h"
#ifndef BUILD_PAX_FORMAT
#include "access/reloptions.h"
#endif
#include "catalog/storage.h"
#include "cdb/cdbvars.h"
#include "commands/cluster.h"
#include "common/file_utils.h"
#include "executor/executor.h"
#include "executor/tuptable.h"
#include "nodes/nodeFuncs.h"
#include "postmaster/syslogger.h"  // for PIPE_CHUNK_SIZE
#include "storage/block.h"
#include "storage/bufmgr.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/relfilenode.h"
#include "storage/smgr.h"
#include "utils/backend_progress.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datum.h"
#include "utils/elog.h"
#include "utils/faultinjector.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/relcache.h"
#include "utils/snapshot.h"
#include "utils/syscache.h"
#include "utils/wait_event.h"

// no header file in cbdb
extern BlockNumber system_nextsampleblock(SampleScanState *node,  // NOLINT
                                          BlockNumber nblocks);
extern bool extractcolumns_from_node(Node *expr, bool *cols,  // NOLINT
                                     AttrNumber natts);
extern int get_partition_for_tuple(PartitionKey key, PartitionDesc partdesc, // NOLINT
                                   Datum *values, bool *isnull);
extern Oid GetDefaultOpClass(Oid type_id, Oid am_id);

#ifdef __cplusplus
}
#pragma GCC diagnostic pop
#endif

#endif  // SRC_CPP_COMM_CBDB_API_H_
