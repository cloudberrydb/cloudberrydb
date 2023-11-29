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

#include "filters.h"
#include "src/dlproxy/headers.h"
#include "access/url.h"
#include "commands/defrem.h"
#include "utils/timestamp.h"
#include "utils/builtins.h"
#include "cdb/cdbvars.h"

/* helper function declarations */
static void add_tuple_desc_httpheader(CHURL_HEADERS headers, Relation rel);
static void add_location_options_httpheader(CHURL_HEADERS headers, GPHDUri *gphduri, transform_callback transform);
static void
add_projection_index_header(CHURL_HEADERS headers,
							int attno,
							char *long_number);
static void
add_projection_desc_httpheader(CHURL_HEADERS headers, List *retrieved_attrs);

/*
 * Add key/value pairs to connection header.
 * These values are the context of the query and used
 * by the remote component.
 */
void
build_http_headers(DlProxyInputData *input, transform_callback transform)
{
	extvar_t       ev;
	CHURL_HEADERS  headers    = input->headers;
	GPHDUri        *gphduri   = input->gphduri;
	Relation       rel        = input->rel;
	char           long_number[sizeof(int32) * 8];
	const char	   *relname;
	char		   *relnamespace = NULL;

	if (rel)
	{
		/* Record fields - name and type of each field */
		add_tuple_desc_httpheader(headers, rel);

		relname = RelationGetRelationName(rel);
		relnamespace = GetNamespaceName(RelationGetNamespace(rel));
	}
	else
	{
		relname = input->relName;
		relnamespace = input->schemaName;
	}

	if (input->retrieved_attrs != NIL)
	{
		bool qualsAreSupported = true;
		List *qualsAttributes =
				 extractDlProxyAttributes(input->quals, &qualsAreSupported);
		/* projection information is incomplete if columns from WHERE clause wasn't extracted */
		/* if any of expressions in WHERE clause is not supported - do not send any projection information at all*/
		if (qualsAreSupported &&
			(qualsAttributes != NIL || list_length(input->quals) == 0))
		{
			add_projection_desc_httpheader(headers, input->retrieved_attrs);
		}
		else
		{
			elog(DEBUG2,
				 "Query will not be optimized to use projection information");
		}
	}

	/* GP cluster configuration */
	external_set_env_vars(&ev, gphduri->uri, false, NULL, NULL, false, 0);

	/* make sure that user identity is known and set, otherwise impersonation by dlagent will be impossible */
	if (!ev.GP_USER || !ev.GP_USER[0])
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("user identity is unknown")));

	churl_headers_append(headers, "X-GP-USER", ev.GP_USER);

	churl_headers_append(headers, "X-GP-SEGMENT-COUNT", ev.GP_SEGMENT_COUNT);
	churl_headers_append(headers, "X-GP-XID", ev.GP_XID);

	pg_ltoa(gp_session_id, long_number);
	churl_headers_append(headers, "X-GP-SESSION-ID", long_number);
	pg_ltoa(gp_command_count, long_number);
	churl_headers_append(headers, "X-GP-COMMAND-COUNT", long_number);

	/* headers for uri data */
	churl_headers_append(headers, "X-GP-DATA-DIR", gphduri->data);
	churl_headers_append(headers, "X-GP-TABLE-NAME", relname);
	churl_headers_append(headers, "X-GP-SCHEMA-NAME", relnamespace);

	/* location options */
	add_location_options_httpheader(headers, gphduri, transform);

	/* full uri */
	churl_headers_append(headers, "X-GP-URI", gphduri->uri);

	/* filters */
	if (input->filterstr && strcmp(input->filterstr, "") != 0)
	{
		churl_headers_append(headers, "X-GP-FILTER", input->filterstr);
		churl_headers_append(headers, "X-GP-HAS-FILTER", "1");
	}
	else
		churl_headers_append(headers, "X-GP-HAS-FILTER", "0");

	// Since we only establish a single connection per segment, we can safely close the connection after
	// the segment completes streaming data.
	churl_headers_override(headers, "Connection", "close");
}

/*
 * The options in the LOCATION statement of "create external table"
 * FRAGMENTER=HdfsDataFragmenter&ACCESSOR=SequenceFileAccessor...
 */
static void
add_location_options_httpheader(CHURL_HEADERS headers,
								GPHDUri *gphduri,
								transform_callback transform)
{
	ListCell   *option = NULL;

	foreach(option, gphduri->options)
	{
		OptionData *data = (OptionData *) lfirst(option);
		char	   *x_gp_key = normalize_key_name(transform(data->key));

		churl_headers_append(headers, x_gp_key, data->value);
		pfree(x_gp_key);
	}
}

/*
 * Report tuple description to remote component
 * Currently, number of attributes, attributes names, types and types modifiers
 * Each attribute has a pair of key/value
 * where X is the number of the attribute
 * X-GP-ATTR-NAMEX - attribute X's name
 * X-GP-ATTR-TYPECODEX - attribute X's type OID (e.g, 16)
 * X-GP-ATTR-TYPENAMEX - attribute X's type name (e.g, "boolean")
 * optional - X-GP-ATTR-TYPEMODX-COUNT - total number of modifier for attribute X
 * optional - X-GP-ATTR-TYPEMODX-Y - attribute X's modifiers Y (types which have precision info, like numeric(p,s))
 *
 * If a column has been dropped from the external table definition, that
 * column will not be reported to the dlproxy server (as if it never existed).
 * For example:
 *
 *  ---------------------------------------------
 * |  col1  |  col2  |  col3 (dropped)  |  col4  |
 *  ---------------------------------------------
 *
 * Col4 will appear as col3 to the dlproxy server as if col3 never existed, and
 * only 3 columns will be reported to dlproxy server.
 */
static void
add_tuple_desc_httpheader(CHURL_HEADERS headers, Relation rel)
{
	int				i, attrIx;
	char			long_number[sizeof(int32) * 8];
	StringInfoData	formatter;
	TupleDesc		tuple;

	initStringInfo(&formatter);

	/* Get tuple description itself */
	tuple = RelationGetDescr(rel);

	/* Iterate attributes */
	for (i = 0, attrIx = 0; i < tuple->natts; ++i)
	{
		FormData_pg_attribute attribute = tuple->attrs[i];

		/* Ignore dropped attributes. */
		if (attribute.attisdropped)
			continue;

		/* Add a key/value pair for attribute name */
		resetStringInfo(&formatter);
		appendStringInfo(&formatter, "X-GP-ATTR-NAME%u", attrIx);
		churl_headers_append(headers, formatter.data, attribute.attname.data);

		/* Add a key/value pair for attribute type */
		resetStringInfo(&formatter);
		appendStringInfo(&formatter, "X-GP-ATTR-TYPECODE%u", attrIx);
		pg_ltoa(attribute.atttypid, long_number);
		churl_headers_append(headers, formatter.data, long_number);

		/* Add a key/value pair for attribute type name */
		resetStringInfo(&formatter);
		appendStringInfo(&formatter, "X-GP-ATTR-TYPENAME%u", attrIx);
		churl_headers_append(headers, formatter.data, TypeOidGetTypename(attribute.atttypid));

		/* Add attribute type modifiers if any */
		if (attribute.atttypmod > -1)
		{
			switch (attribute.atttypid)
			{
				case NUMERICOID:
				case NUMERIC_ARRAY_OID:
					{
						resetStringInfo(&formatter);
						appendStringInfo(&formatter, "X-GP-ATTR-TYPEMOD%u-COUNT", attrIx);
						pg_ltoa(2, long_number);
						churl_headers_append(headers, formatter.data, long_number);


						/* precision */
						resetStringInfo(&formatter);
						appendStringInfo(&formatter, "X-GP-ATTR-TYPEMOD%u-%u", attrIx, 0);
						pg_ltoa((attribute.atttypmod >> 16) & 0xffff, long_number);
						churl_headers_append(headers, formatter.data, long_number);

						/* scale */
						resetStringInfo(&formatter);
						appendStringInfo(&formatter, "X-GP-ATTR-TYPEMOD%u-%u", attrIx, 1);
						pg_ltoa((attribute.atttypmod - VARHDRSZ) & 0xffff, long_number);
						churl_headers_append(headers, formatter.data, long_number);
						break;
					}
				case CHAROID:
				case CHAR_ARRAY_OID:
				case BPCHAROID:
				case BPCHAR_ARRAY_OID:
				case VARCHAROID:
				case VARCHAR_ARRAY_OID:
					{
						resetStringInfo(&formatter);
						appendStringInfo(&formatter, "X-GP-ATTR-TYPEMOD%u-COUNT", attrIx);
						pg_ltoa(1, long_number);
						churl_headers_append(headers, formatter.data, long_number);

						resetStringInfo(&formatter);
						appendStringInfo(&formatter, "X-GP-ATTR-TYPEMOD%u-%u", attrIx, 0);
						pg_ltoa((attribute.atttypmod - VARHDRSZ), long_number);
						churl_headers_append(headers, formatter.data, long_number);
						break;
					}
				case VARBITOID:
				case VARBIT_ARRAY_OID:
				case BITOID:
				case BIT_ARRAY_OID:
				case TIMESTAMPOID:
				case TIMESTAMP_ARRAY_OID:
				case TIMESTAMPTZOID:
				case TIMESTAMPTZ_ARRAY_OID:
				case TIMEOID:
				case TIME_ARRAY_OID:
				case TIMETZOID:
				case TIMETZ_ARRAY_OID:
					{
						resetStringInfo(&formatter);
						appendStringInfo(&formatter, "X-GP-ATTR-TYPEMOD%u-COUNT", attrIx);
						pg_ltoa(1, long_number);
						churl_headers_append(headers, formatter.data, long_number);

						resetStringInfo(&formatter);
						appendStringInfo(&formatter, "X-GP-ATTR-TYPEMOD%u-%u", attrIx, 0);
						pg_ltoa((attribute.atttypmod), long_number);
						churl_headers_append(headers, formatter.data, long_number);
						break;
					}
				case INTERVALOID:
				case INTERVAL_ARRAY_OID:
					{
						resetStringInfo(&formatter);
						appendStringInfo(&formatter, "X-GP-ATTR-TYPEMOD%u-COUNT", attrIx);
						pg_ltoa(1, long_number);
						churl_headers_append(headers, formatter.data, long_number);

						resetStringInfo(&formatter);
						appendStringInfo(&formatter, "X-GP-ATTR-TYPEMOD%u-%u", attrIx, 0);
						pg_ltoa(INTERVAL_PRECISION(attribute.atttypmod), long_number);
						churl_headers_append(headers, formatter.data, long_number);
						break;
					}
				default:
					elog(DEBUG5, "add_tuple_desc_httpheader: unsupported type %d ", attribute.atttypid);
					break;
			}
		}
		attrIx++;
	}

	/* Convert the number of attributes to a string */
	pg_ltoa(attrIx, long_number);
	churl_headers_append(headers, "X-GP-ATTRS", long_number);

	pfree(formatter.data);
}

/*
 * Report projection description to the remote component, the indices of
 * dropped columns do not get reported, as if they never existed, and
 * column indices that follow dropped columns will be shifted by the number
 * of dropped columns that precede it. For example,
 *
 *  ---------------------------------------------
 * |  col1  |  col2 (dropped)  |  col3  |  col4  |
 *  ---------------------------------------------
 *
 * Let's assume that col1 and col4 are projected, the reported projected
 * indices will be 0, 2. This is because we use 0-based indexing and because
 * col2 was dropped, the indices for col3 and col4 get shifted by -1.
 */
static void
add_projection_desc_httpheader(CHURL_HEADERS headers, List *retrieved_attrs)
{
	ListCell   *lc1 = NULL;
	char		long_number[sizeof(int32) * 8];

	if (list_length(retrieved_attrs) == 0)
		return;

	foreach(lc1, retrieved_attrs)
	{
		int			attno = lfirst_int(lc1);

		/* zero-based index in the server side */
		add_projection_index_header(headers, attno - 1, long_number);
	}

	/* Convert the number of projection columns to a string */
	pg_ltoa(retrieved_attrs->length, long_number);
	churl_headers_append(headers, "X-GP-ATTRS-PROJ", long_number);
}

/*
 * Adds the projection index header for the given attno
 */
static void
add_projection_index_header(CHURL_HEADERS headers,
							int attno,
							char *long_number)
{
	pg_ltoa(attno, long_number);
	churl_headers_append(headers, "X-GP-ATTRS-PROJ-IDX", long_number);
}
