/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#include "postgres.h"

#include <jansson.h>
#include "headers.h"
#include "protocol.h"
#include "filters.h"
#include "deparse.h"

#include "access/table.h"
#include "cdb/cdbtm.h"
#include "cdb/cdbvars.h"
#include "utils/guc.h"
#include "datalake.h"
#include "optimizer/optimizer.h"

#include "src/datalake_def.h"

#define BUFFER_SIZE 4096

typedef struct
{
	char *typeName;
	Oid  typeOid;
} TypeInfoElt;

/* helper function declarations */
static void build_uri_for_read(gphadoop_context *context);
static void add_querydata_to_http_headers(gphadoop_context *context, transform_callback transform);
static size_t fill_buffer(gphadoop_context *context, char *start, size_t size);

static const TypeInfoElt typeInfoElts[] = {
	{"int4", INT4OID},
	{"int8", INT8OID},
	{"bool", BOOLOID},
	{"float4", FLOAT4OID},
	{"float8", FLOAT8OID},
	{"text", TEXTOID},
	{"bytea", BYTEAOID},
	{"timestamp", TIMESTAMPOID},
	{"timestamptz", TIMESTAMPTZOID},
	{"date", DATEOID},
	{"time", TIMEOID},
	{"uuid", UUIDOID},
	{"numeric", NUMERICOID}
};

static Oid
getFieldTypeOidByName(const char *typeName)
{
	int i;

	for (i = 0; i < lengthof(typeInfoElts); i++)
	{
		if (!strcmp(typeInfoElts[i].typeName, typeName))
			return typeInfoElts[i].typeOid;
	}

	ereport(ERROR,
			(errcode(ERRCODE_DATATYPE_MISMATCH),
			 errmsg("unsupported data type \"%s\"", typeName)));
}

static char *
get_catalog_type(char *profile, List *locations)
{
	GPHDUri *uri;
	char    *catalogType;

	uri = parseGPHDUri(strVal(linitial(locations)));
	catalogType = pstrdup(getOptionValue(uri, CATALOG_TYPE));

	if (!(pg_strcasecmp(profile, "hudi") == 0 &&
			pg_strcasecmp(catalogType, "hadoop") == 0))
	{
		List *coreOptions = list_make1(TABLE_IDENTIFIER);
		GPHDUri_verify_core_options_exist(uri, coreOptions);
		list_free(coreOptions);
	}

	freeGPHDUri(uri);

	return catalogType;
}

static const char *
transform_datalake_options(const char *key)
{
	int i;
	static option_mapping configElts[] = {
		{"server_name", "server"},
		{"table_identifier", "data-source"},
		{"split_size", "split-size"},
		{"query_type", "query-type"},
		{"metadata_table_enable", "metadata-table-enable"}
	};

	for (i = 0; i < lengthof(configElts); i++)
	{
		if (!strcmp(configElts[i].option_name, key))
			return configElts[i].property_name;
	}

	return key;
}

static List *
parseSchemaResponse(char *buffer, size_t buffer_size)
{
	List         *result = NIL;
	json_t       *jroot;
	json_t       *jfields;
	json_error_t  jerror;
	int           i;

	jroot = json_loadb(buffer, buffer_size, 0, &jerror);
	if (jroot == NULL)
		elog(ERROR, "failed to decode message:\"%s\", errMessage: %s line: %d",
					pnstrdup(buffer, buffer_size), jerror.text, jerror.line);

	jfields = json_object_get(jroot, "fields");
	for (i = 0; i < json_array_size(jfields); i++)
	{
		json_t *jmods;
		json_t *jfield = json_array_get(jfields, i);
		TableFieldDefination *columnDef = palloc0(sizeof(TableFieldDefination));

		columnDef->fieldName = pstrdup(json_string_value(json_object_get(jfield, "name")));
		columnDef->fieldTypeName = pstrdup(json_string_value(json_object_get(jfield, "type")));
		columnDef->fieldTypeOid = getFieldTypeOidByName(columnDef->fieldTypeName);

		jmods = json_object_get(jfield, "modifiers");
		if (jmods)
		{
			columnDef->fieldTypeMod1 = atoi(json_string_value(json_array_get(jmods, 0)));
			columnDef->fieldTypeMod2 = atoi(json_string_value(json_array_get(jmods, 1)));
		}

		result = lappend(result, columnDef);
	}

	json_decref(jroot);

	return result;
}

/*
 * De-allocates context and dependent structures.
 */
void
destroy_context(gphadoop_context *context, bool afterError)
{
	if (context == NULL)
		return;

	churl_cleanup(context->churl_handle, afterError);
	context->churl_handle = NULL;

	churl_headers_cleanup(context->churl_headers);
	context->churl_headers = NULL;

	if (context->relation)
		heap_close(context->relation, NoLock);

	if (context->gphd_uri != NULL)
	{
		freeGPHDUri(context->gphd_uri);
		context->gphd_uri = NULL;
	}

	if (context->filterstr != NULL)
	{
		pfree(context->filterstr);
		context->filterstr = NULL;
	}

	pfree(context->uri.data);
	pfree(context->buffer);
	pfree(context);
}

/*
 * Allocates context and sets values for the segment
 */

static gphadoop_context *
create_context_(Oid relid,
				Index relno,
				char *relName,
				char *schemaName,
				List *restrictInfo,
				List *targetList,
				const char *uriStr,
				transform_callback transform)
{
	List *remoteConds;
	List *localConds;
	Bitmapset  *attrsUsed = NULL;
	/* parse and set uri */
	GPHDUri        *uri = parseGPHDUri(uriStr);

	/* set context */
	gphadoop_context *context = palloc0(sizeof(gphadoop_context));

	context->gphd_uri = uri;
	initStringInfo(&context->uri);

	context->relName = relName;
	context->schemaName = schemaName;

	/*
	 * Identify which baserestrictinfo clauses can be sent to the remote
	 * server and which can't.
	 */
	classifyConditions(restrictInfo, &remoteConds, &localConds);
	context->filterstr = serializeDlProxyFilterQuals(remoteConds);
	context->quals = localConds;

	if (OidIsValid(relid))
	{
		ListCell *lc;
		List *retrieved_attrs = NIL;

		context->relation = table_open(relid, NoLock);

		/*
		 * Identify which attributes will need to be retrieved from the remote
		 * server
		 */
		pull_varattnos((Node *) targetList, relno, &attrsUsed);

		foreach(lc, localConds)
		{
			Node *rinfo = (Node *) lfirst(lc);

			pull_varattnos((Node *) rinfo, relno, &attrsUsed);
		}

		deparseTargetList(context->relation, attrsUsed, &retrieved_attrs);
		context->retrieved_attrs = retrieved_attrs;
	}

	build_uri_for_read(context);
	context->churl_headers = churl_headers_init();
	add_querydata_to_http_headers(context, transform);

	context->buffer = palloc(BUFFER_SIZE);
	context->buffer_size = BUFFER_SIZE;

	return context;
}

gphadoop_context *
create_context(Oid relid, const char *uriStr, transform_callback transform)
{
	return create_context_(relid, 0, NULL, NULL, NULL, NULL, uriStr, transform);
}

gphadoop_context *
create_context2(char *relName, char *schemaName, const char *uriStr, transform_callback transform)
{
	return create_context_(InvalidOid, 0, relName, schemaName, NULL, NULL, uriStr, transform);
}


gphadoop_context *
create_fragment_context(Oid relid,
						Index relno,
						List *restrictInfo,
						List *targetList,
						const char *uriStr,
						transform_callback transform)
{
	return create_context_(relid, relno, NULL, NULL, restrictInfo, targetList, uriStr, transform);
}

void
doRPC(gphadoop_context *context)
{
	size_t n;

	context->churl_handle = churl_init_download(context->uri.data, context->churl_headers);

	/* read some bytes to make sure the connection is established */
	churl_read_check_connectivity(context->churl_handle);

	while ((n = fill_buffer(context,
							context->buffer + context->buffer_pos,
							context->buffer_size - context->buffer_pos)) != 0)
	{
		context->buffer_pos += n;

		if (context->buffer_pos == context->buffer_size)
		{
			context->buffer_size *= 2;
			context->buffer = repalloc(context->buffer, context->buffer_size);
		}
	}

	context->completed = true;

	/* check if the connection terminated with an error */
	churl_read_check_connectivity(context->churl_handle);
}

/*
 * Format the URI for reading by adding dlproxy service endpoint details
 */
static void
build_uri_for_read(gphadoop_context *context)
{
	resetStringInfo(&context->uri);
	appendStringInfo(&context->uri, "http://%s/%s/read",
					 get_authority(), DLPROXY_SERVICE_PREFIX);

	if ((LOG >= log_min_messages) || (LOG >= client_min_messages))
	{
		appendStringInfo(&context->uri, "?trace=true");
	}

	elog(DEBUG2, "dlproxy: uri %s for read", context->uri.data);
}

/*
 * Add key/value pairs to connection header. These values are the context of the query and used
 * by the remote component.
 */
static void
add_querydata_to_http_headers(gphadoop_context *context, transform_callback transform)
{
	DlProxyInputData inputData = {0};

	inputData.headers   = context->churl_headers;
	inputData.gphduri   = context->gphd_uri;
	inputData.rel       = context->relation;
	inputData.filterstr = context->filterstr;
	inputData.retrieved_attrs = context->retrieved_attrs;
	inputData.quals = context->quals;
	inputData.relName = context->relName;
	inputData.schemaName     = context->schemaName;
	build_http_headers(&inputData, transform);
}

/*
 * Read data from churl until the buffer is full or there is no more data to be read
 */
static size_t
fill_buffer(gphadoop_context *context, char *start, size_t size)
{

	size_t		n;
	char	   *ptr = start;
	char	   *end = ptr + size;

	while (ptr < end)
	{
		n = churl_read(context->churl_handle, ptr, end - ptr);
		if (n == 0)
			break;

		ptr += n;
	}

	return ptr - start;
}

List *
internal_get_external_fragments(char *profile,
								Oid relid,
								Index relno,
								List *restrictInfo,
								List *targetList,
								List *locations,
								parse_callback parseFn)
{
	List *result = NIL;
	volatile gphadoop_context *context = NULL;

	PG_TRY();
	{
		char *catalogType = get_catalog_type(profile, locations);

		context = create_fragment_context(relid,
										  relno,
										  restrictInfo,
										  targetList,
										  strVal(linitial(locations)),
										  transform_datalake_options);
		churl_headers_append(context->churl_headers, "X-GP-OPTIONS-PROFILE", profile);

		if (pg_strcasecmp(catalogType, "hive") == 0)
			churl_headers_append(context->churl_headers, "X-GP-OPTIONS-CONFIG", "gphive.conf0gphdfs.conf");
		else
			churl_headers_append(context->churl_headers, "X-GP-OPTIONS-CONFIG", "gphdfs.conf");

		churl_headers_append(context->churl_headers, "X-GP-OPTIONS-CATALOG-TYPE", catalogType);
		pfree(catalogType);

		churl_headers_append(context->churl_headers, "X-GP-OPTIONS-SCAN-TYPE", "snapshot");
		churl_headers_append(context->churl_headers, "X-GP-OPTIONS-METHOD", "getFragments");

		doRPC((gphadoop_context *) context);
		result = parseFn(context->buffer, context->buffer_pos);

		destroy_context((gphadoop_context *) context, false);
		context = NULL;
	}
	PG_CATCH();
	{
		if (context)
			destroy_context((gphadoop_context *) context, true);

		PG_RE_THROW();
	}
	PG_END_TRY();

	return result;
}

List *
get_external_fragments(Oid relid,
					   Index relno,
					   List *restrictInfo,
					   List *targetList,
					   List *locations,
					   char* formatType,
					   bool isWritable)
{
	if (isWritable)
		return NIL;

	if (FORMAT_IS_ICEBERG(formatType))
		//return iceberg_get_external_fragments(relid, relno, restrictInfo, targetList, locations);
		return NIL;
	else if (FORMAT_IS_HUDI(formatType))
		//return hudi_get_external_fragments(relid, relno, restrictInfo, targetList, locations);
		return NIL;
	else
		return hive_get_external_partitions(relid, locations, formatType);
}

List *
get_external_schema(char *profile, char *relName, char *schemaName, List *locations)
{
	List *result = NIL;
	volatile gphadoop_context *context = NULL;

	PG_TRY();
	{
		char *catalogType = get_catalog_type(profile, locations);

		context = create_context2(relName, schemaName, strVal(linitial(locations)), transform_datalake_options);
		churl_headers_append(context->churl_headers, "X-GP-OPTIONS-PROFILE", profile);

		if (pg_strcasecmp(catalogType, "hive") == 0)
			churl_headers_append(context->churl_headers, "X-GP-OPTIONS-CONFIG", "gphive.conf0gphdfs.conf");
		else
			churl_headers_append(context->churl_headers, "X-GP-OPTIONS-CONFIG", "gphdfs.conf");

		churl_headers_append(context->churl_headers, "X-GP-OPTIONS-CATALOG-TYPE", catalogType);
		pfree(catalogType);

		churl_headers_append(context->churl_headers, "X-GP-OPTIONS-METHOD", "getSchema");

		doRPC((gphadoop_context *) context);
		result = parseSchemaResponse(context->buffer, context->buffer_pos);

		destroy_context((gphadoop_context *) context, false);
		context = NULL;
	}
	PG_CATCH();
	{
		if (context)
			destroy_context((gphadoop_context *) context, true);

		PG_RE_THROW();
	}
	PG_END_TRY();

	return result;
}


