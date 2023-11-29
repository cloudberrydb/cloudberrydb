#include "postgres.h"

#include "access/appendonlywriter.h"
#include "access/relation.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace_d.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbdisp_query.h"
#include "postmaster/postmaster.h"
#include "foreign/foreign.h"
#include "fmgr.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/inval.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/varlena.h"
#include "utils/syscache.h"
#include "string.h"
#include <gopher/gopher.h>

#include "src/datalake_option.h"
#include "src/common/fileSystemWrapper.h"



PG_FUNCTION_INFO_V1(hdw_gopher_free_all_cache_wrapper);
PG_FUNCTION_INFO_V1(hdw_gopher_cache_free_relation_name_wrapper);


extern Datum hdw_gopher_free_all_cache_wrapper(PG_FUNCTION_ARGS);
extern Datum hdw_gopher_cache_free_relation_name_wrapper(PG_FUNCTION_ARGS);
Datum hdw_ao_free_cache_name(PG_FUNCTION_ARGS);
Datum hdw_free_all_ao_cache(PG_FUNCTION_ARGS);
void hdw_GetGopherSocketPath(char *dest, size_t size);
int GopherRemoveFileMetaUnderAllUfsPath(char* prefix, int mRecursive, int mForce);
static int freeAOCachedFiles(Relation rel);
static int freeForeignTableCache(Relation relation);
int GopherRemoveFileMetaUnderGphdfs(dataLakeOptions *options);
int GopherRemoveFileMetaUnderGphdfsUfsPath(int ufsId, char* prefix, int mRecursive, int mForce);

#ifdef FDDEBUG
#define DO_DB(A) \
	do { \
		int			_do_db_save_errno = errno; \
		A; \
		errno = _do_db_save_errno; \
	} while (0)
#else
#define DO_DB(A) \
	((void) 0)
#endif

Datum
hdw_gopher_free_all_cache_wrapper(PG_FUNCTION_ARGS)
{
	Datum returnValue = hdw_free_all_ao_cache(fcinfo);

	PG_RETURN_DATUM(returnValue);
}

Datum
hdw_gopher_cache_free_relation_name_wrapper(PG_FUNCTION_ARGS)
{
	Datum returnValue = hdw_ao_free_cache_name(fcinfo);

	PG_RETURN_DATUM(returnValue);
}

Datum
hdw_free_all_ao_cache(PG_FUNCTION_ARGS)
{
	if (Gp_role == GP_ROLE_EXECUTE && GpIdentity.segindex == -1)
		elog(ERROR, "This query is not currently supported by GPDB.");

	if(GopherRemoveFileMetaUnderAllUfsPath("/", 1, 1) == -1)
	{
		ereport(ERROR, (errcode(ERRCODE_GP_COMMAND_ERROR), errmsg("could not delete gopher cache")));
	}

	if (Gp_role == GP_ROLE_DISPATCH)
		CdbDispatchCommand("SELECT gp_toolkit.__gopher_free_all_cache()", DF_CANCEL_ON_ERROR, NULL);

	PG_RETURN_INT32((int) 1);
}

/*
 * GetGopherSocketPath
 * Gopher socket path
 */
void
hdw_GetGopherSocketPath(char *dest, size_t size)
{
	snprintf(dest, size, "/tmp/.s.gopher.%d", PostPortNumber);
}

/*
 * GopherRemoveFileMetaUnderAllUfsPath
 * cleans up gopher cache and gopher metadata under the all ufs path
 */
int
GopherRemoveFileMetaUnderAllUfsPath(char* prefix, int mRecursive, int mForce)
{
	char hostAddress[1024] = {0};
	hdw_GetGopherSocketPath(hostAddress, sizeof(hostAddress));
	gopherAdmin admin = gopherCreateAdmin(hostAddress);

	int nEntries = 0;
	gopherUfsInfo *ufsInfos = gopherListUfsInfo(admin, &nEntries);
	for (int i = 0; i < nEntries; i++)
	{
		int ufsId = ufsInfos[i].ufs_id;
		if (gopherRemovePath(admin, ufsId, prefix, mRecursive, mForce))
		{
			DO_DB(elog(LOG, "GopherRemoveFileMetaUnderAllUfsPath: cannot remove gopherServer "
				"metadata %s.", gopherGetLastError()));
			return -1;
		}
	}
	gopherFreeUfsInfo(ufsInfos, nEntries);
	if (gopherDeleteAdmin(admin))
	{
		DO_DB(elog(LOG, "GopherRemoveFileMetaUnderAllUfsPath: cannot remove "
			"gopherAdmin %s.", gopherGetLastError()));
		return -1;
	}

	DO_DB(elog(LOG, "GopherRemoveFileMetaUnderAllUfsPath: success"));
	return 0;
}

Datum
hdw_ao_free_cache_name(PG_FUNCTION_ARGS)
{
	text *relname = PG_GETARG_TEXT_P(0);
	Relation rel;
	RangeVar *relrv;
	int result = 0;

	if (Gp_role == GP_ROLE_EXECUTE && GpIdentity.segindex == -1)
		elog(ERROR, "This query is not currently supported by GPDB.");

	relrv = makeRangeVarFromNameList(textToQualifiedNameList(relname));

	if (Gp_role == GP_ROLE_EXECUTE && relrv->schemaname != NULL)
	{
		Oid namespaceId;
		Oid relOid;

		AcceptInvalidationMessages();

		namespaceId = GetSysCacheOid1(NAMESPACENAME, Anum_pg_namespace_oid, CStringGetDatum(relrv->schemaname));
		relOid = get_relname_relid(relrv->relname, namespaceId);
		if (!OidIsValid(relOid))
		{
			DO_DB(elog(LOG, "cannot free cache. %s oid is invalid.",
				DatumGetCString(DirectFunctionCall1(textout, PointerGetDatum(relname)))));
			PG_RETURN_INT32((int) result);
		}

		rel = try_relation_open(relOid, AccessShareLock, false);
	}
	else
		rel = relation_openrv_extended(relrv, AccessShareLock, false);

	if (!RelationIsValid(rel))
	{
		DO_DB(elog(LOG, "cannot free cache. %s relation is invalid.",
			DatumGetCString(DirectFunctionCall1(textout, PointerGetDatum(relname)))));
		PG_RETURN_INT32((int) result);
	}

	if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
		result = freeAOCachedFiles(rel);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		char * rawname;
		StringInfoData buffer;
		initStringInfo(&buffer);

		rawname = DatumGetCString(DirectFunctionCall1(textout, PointerGetDatum(relname)));
		appendStringInfo(&buffer, "select gp_toolkit.__gopher_cache_free_relation_name('%s')", rawname);
		CdbDispatchCommand(buffer.data, DF_CANCEL_ON_ERROR, NULL);
		pfree(buffer.data);
		/* master set it to true */
		result = 1;
	}

	relation_close(rel, AccessShareLock);
	PG_RETURN_INT32((int) result);
}

static int
freeAOCachedFiles(Relation rel)
{
	if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
	{
		return freeForeignTableCache(rel);
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("The function 'freeAOCachedFiles' only supports foreign table") ));
	}
	return 0;
}

static int
freeForeignTableCache(Relation relation)
{
	Oid foreigntableid;

	foreigntableid = RelationGetRelid(relation);

	dataLakeOptions *options = getOptions(foreigntableid);

	if (Gp_role == GP_ROLE_DISPATCH)
		return 0;

	int ret = GopherRemoveFileMetaUnderGphdfs(options);
	if (ret == -1)
	{
		elog(ERROR, "failed to release \"%s\" cache", RelationGetRelationName(relation));
	}
	return ret;
}

/**
 * GopherRemoveFileMetaUnderGphdfs
 * The interface is used for external tables gphdfs, cleans up gopher cache
 * and gopher metadata under the hdfs
 */
int
GopherRemoveFileMetaUnderGphdfs(dataLakeOptions *options)
{
	int mRecursive = 1;
	int mForce = 1;

	gopherConfig* conf = createGopherConfig((void*)options->gopher);
	ossFileStream stream = createFileSystem(conf);
	freeGopherConfig(conf);
	int ufsId = getUfsId(stream);
	if (ufsId < 0)
	{
		return -1;
	}
	gopherDestroyHandle(stream);
	return GopherRemoveFileMetaUnderGphdfsUfsPath(ufsId, options->filePath, mRecursive, mForce);
}

/*
 * GopherRemoveFileMetaUnderGphdfsUfsPath
 * cleans up gopher cache and gopher metadata under the gphdfs ufs path
 */
int
GopherRemoveFileMetaUnderGphdfsUfsPath(int ufsId, char* prefix, int mRecursive, int mForce)
{
	char hostAddress[MAXPGPATH + 1] = {0};
	hdw_GetGopherSocketPath(hostAddress, sizeof(hostAddress));
	gopherAdmin admin = gopherCreateAdmin(hostAddress);
	if (gopherRemovePath(admin, ufsId, prefix, mRecursive, mForce) == -1)
	{
		DO_DB(elog(LOG, "GopherRemoveFileMetaUnderGphdfsUfsPath: cannot remove gopherServer "
			"metadata %s.", gopherGetLastError()));
		return -1;
	}

	if (gopherDeleteAdmin(admin))
	{
		DO_DB(elog(LOG, "GopherRemoveFileMetaUnderGphdfsUfsPath: cannot remove "
			"gopherAdmin %s.", gopherGetLastError()));
		return -1;
	}

	DO_DB(elog(LOG, "GopherRemoveFileMetaUnderGphdfsUfsPath: success"));
	return 1;
}