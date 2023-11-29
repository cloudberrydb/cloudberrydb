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
 */

#ifndef _PROTOCOL_H_
#define _PROTOCOL_H_

#include "libchurl.h"
#include "uriparser.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "utils/rel.h"
#include "src/datalake_def.h"

/*
 * Context for single query execution by dlproxy
 */
typedef struct
{
	CHURL_HEADERS  churl_headers;
	CHURL_HANDLE   churl_handle;
	GPHDUri        *gphd_uri;
	StringInfoData uri;
	Relation       relation;
	char           *filterstr;
	List           *retrieved_attrs;
	List           *quals;
	bool           completed;
	char           *buffer;
	size_t          buffer_pos;
	size_t          buffer_size;
	char           *relName;
	char           *schemaName;
} gphadoop_context;

typedef struct
{
	char *option_name;
	char *property_name;
} option_mapping;

typedef List * (*parse_callback) (char *buffer, size_t buffer_size);

extern List *
hive_get_external_partitions(Oid relid,
							 List *locations,
							 char *formatType);

// extern List *
// iceberg_get_external_fragments(Oid relid,
// 							   Index relno,
// 							   List *restrictInfo,
// 							   List *targetList,
// 							   List *locations);

// extern List *
// hudi_get_external_fragments(Oid relid,
// 							Index relno,
// 							List *restrictInfo,
// 							List *targetList,
// 							List *locations);

extern List *
internal_get_external_fragments(char *profile,
								Oid relid,
								Index relno,
								List *restrictInfo,
								List *targetList,
								List *locations,
								parse_callback parseFn);

extern void
destroy_context(gphadoop_context *context, bool afterError);

extern gphadoop_context *
create_context(Oid relid, const char *uriStr, transform_callback transformFn);

extern gphadoop_context *
create_context2(char *relName, char *schemaName, const char *uriStr, transform_callback transformFn);

extern gphadoop_context *
create_fragment_context(Oid relid,
						Index relno,
						List *restrictInfo,
						List *retrievedAttrs,
						const char *uriStr,
						transform_callback transformFn);

extern void
doRPC(gphadoop_context *context);



#endif							/* _PROTOCOL_H_ */
