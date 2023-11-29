#include "utils.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "catalog/pg_namespace.h"
#include "utils/formatting.h"
#include "utils/syscache.h"

/*
 * Full name of the HEADER KEY expected by the dlproxy service
 * Converts input string to upper case and prepends "X-GP-OPTIONS-" string
 * This will be used for all user defined parameters to be isolate from internal parameters
 */
char *
normalize_key_name(const char *key)
{
	if (!key || strlen(key) == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("internal error in utils.c:normalize_key_name. Parameter key is null or empty.")));
	}

	return psprintf("X-GP-OPTIONS-%s", asc_toupper(pstrdup(key), strlen(key)));
}

/* Concatenate multiple literal strings using stringinfo */
char *
concat(int num_args,...)
{
	va_list		ap;
	StringInfoData str;

	initStringInfo(&str);

	va_start(ap, num_args);

	for (int i = 0; i < num_args; i++)
	{
		appendStringInfoString(&str, va_arg(ap, char *));
	}
	va_end(ap);

	return str.data;
}

/* Get authority (host:port) for the dlproxy server URL */
char *
get_authority(void)
{
	return psprintf("%s:%d", get_dlproxy_host(), get_dlproxy_port());
}

/* Returns the dlproxy Host defined in the DLPROXY_HOST
 * environment variable or the default when undefined
 */
const char *
get_dlproxy_host(void)
{
	const char *hStr = getenv(ENV_DLPROXY_HOST);
	if (hStr)
		elog(DEBUG3, "read environment variable %s=%s", ENV_DLPROXY_HOST, hStr);
	else
		elog(DEBUG3, "environment variable %s was not supplied", ENV_DLPROXY_HOST);
	return hStr ? hStr : DLPROXY_DEFAULT_HOST;
}

/* Returns the dlproxy Port defined in the DLPROXY_PORT
 * environment variable or the default when undefined
 */
const int
get_dlproxy_port(void)
{
	char *endptr = NULL;
	char *pStr = getenv(ENV_DLPROXY_PORT);
	int port = DLPROXY_DEFAULT_PORT;

	if (pStr) {
		port = (int) strtol(pStr, &endptr, 10);

		if (pStr == endptr)
			elog(ERROR, "unable to parse dlproxy port number %s=%s", ENV_DLPROXY_PORT, pStr);
		else
			elog(DEBUG3, "read environment variable %s=%s", ENV_DLPROXY_PORT, pStr);
	}
	else
	{
		elog(DEBUG3, "environment variable %s was not supplied", ENV_DLPROXY_PORT);
	}

	return port;
}

/* Returns the namespace (schema) name for a given namespace oid */
char *
GetNamespaceName(Oid nsp_oid)
{
	HeapTuple	tuple;
	Datum		nspnameDatum;
	bool		isNull;

	tuple = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(nsp_oid));
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_SCHEMA),
						errmsg("schema with OID %u does not exist", nsp_oid)));

	nspnameDatum = SysCacheGetAttr(NAMESPACEOID, tuple, Anum_pg_namespace_nspname,
								   &isNull);

	ReleaseSysCache(tuple);

	return DatumGetCString(nspnameDatum);
}

/*
 * TypeOidGetTypename
 * Get the name of the type, given the OID
 */
char *
TypeOidGetTypename(Oid typid)
{

	Assert(OidIsValid(typid));

	HeapTuple	typtup = SearchSysCache(TYPEOID,
							ObjectIdGetDatum(typid),
							0, 0, 0);
	if (!HeapTupleIsValid(typtup))
		elog(ERROR, "cache lookup failed for type %u", typid);

	Form_pg_type typform = (Form_pg_type) GETSTRUCT(typtup);
	char	   *typname = psprintf("%s", NameStr(typform->typname));
	ReleaseSysCache(typtup);

	return typname;
}
