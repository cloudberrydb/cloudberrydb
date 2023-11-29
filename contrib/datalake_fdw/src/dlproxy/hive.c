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
#include "datalake.h"
#include "headers.h"
#include "protocol.h"

#include "cdb/cdbtm.h"
#include "cdb/cdbvars.h"
#include "utils/guc.h"


static const char *
transform_hive_options(const char *key)
{
	int i;
	static option_mapping configElts[] = {
		{"hive_cluster_name", "server"},
		{"datasource", "data-source"}
	};

	for (i = 0; i < lengthof(configElts); i++)
	{
		if (!strcmp(configElts[i].option_name, key))
			return configElts[i].property_name;
	}

	return key;
}

List *
parsePartitionResponse(char *buffer, size_t buffer_size)
{
	List         *result = NIL;
	json_t       *jroot;
	json_t       *jmetadata;
	json_t       *jvalues;
	json_error_t  jerror;
	int           i;
	int           j;

	jroot = json_loadb(buffer, buffer_size, 0, &jerror);
	if (jroot == NULL)
		elog(ERROR, "failed to decode message:\"%s\", errMessage: %s line: %d",
					pnstrdup(buffer, buffer_size), jerror.text, jerror.line);

	for (i = 0; i < json_array_size(jroot); i++)
	{
		jmetadata = json_object_get(json_array_get(jroot, i), "metadata");
		jvalues = json_object_get(jmetadata, "values");

		List *values = NIL;
		for (j = 0; j < json_array_size(jvalues); j++)
		{
			const char *value = json_string_value(json_array_get(jvalues, j));
			values = lappend(values, makeString(pstrdup(value)));
		}

		if (list_length(values) > 0)
			result = lappend(result, values);
	}
	json_decref(jroot);

	return result;
}

void
freePartitionList(List *partitions)
{
	ListCell *lci;
	ListCell *lco;

	foreach(lco, partitions)
	{
		List *values = (List*) lfirst(lco);

		foreach(lci, values)
		{
			char *value = strVal(lfirst(lci));
			pfree(value);
		}

		list_free_deep(values);
	}

	list_free(partitions);
}

List *
hive_get_external_partitions(Oid relid, List *locations, char* formatType)
{
	List *result = NIL;
	volatile gphadoop_context *context = NULL;

	PG_TRY();
	{
		context = create_context(relid, strVal(linitial(locations)), transform_hive_options);
		churl_headers_append(context->churl_headers, "X-GP-OPTIONS-PROFILE", "hive");
		churl_headers_append(context->churl_headers, "X-GP-OPTIONS-CONFIG", "gphive.conf");
		churl_headers_append(context->churl_headers, "X-GP-OPTIONS-METHOD", "getPartitions");
		churl_headers_append(context->churl_headers, "X-GP-OPTIONS-CATALOG-TYPE", "hive");

		doRPC((gphadoop_context *) context);
		result = parsePartitionResponse(context->buffer, context->buffer_pos);
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
