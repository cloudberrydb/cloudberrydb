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

#ifndef _HEADERS_H_
#define _HEADERS_H_

#include "libchurl.h"
#include "uriparser.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "utils/rel.h"

typedef const char * (*transform_callback) (const char *key);

/*
 * Contains the data necessary to build the HTTP headers required for calling on the dlproxy service
 */
typedef struct sDlProxyInputData
{
	CHURL_HEADERS  headers;
	GPHDUri        *gphduri;
	Relation       rel;
	char           *filterstr;
	List           *retrieved_attrs;
	List           *quals;
	char           *relName;
	char           *schemaName;
} DlProxyInputData;

/*
 * Adds the headers necessary for dlproxy service call
 */
extern void build_http_headers(DlProxyInputData *input, transform_callback transform);

#define CHAR_ARRAY_OID 1002
#define BPCHAR_ARRAY_OID 1014
#define VARCHAR_ARRAY_OID 1015
#define TIMESTAMP_ARRAY_OID 1115
#define TIME_ARRAY_OID 1183
#define TIMESTAMPTZ_ARRAY_OID 1185
#define INTERVAL_ARRAY_OID 1187
#define NUMERIC_ARRAY_OID 1231
#define TIMETZ_ARRAY_OID 1270
#define BIT_ARRAY_OID 1561
#define VARBIT_ARRAY_OID 1563

#endif							/* _HEADERS_H_ */
