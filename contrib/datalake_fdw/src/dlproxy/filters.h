/*
 * filters.h
 *
 * Header for handling push down of supported scan level filters to dlproxy.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.	See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
#ifndef _FILTERS_H_
#define _FILTERS_H_

#include "postgres.h"
#include "access/attnum.h"
#include "executor/executor.h"
#include "nodes/pg_list.h"


/*
 * each supported operator has a code that will describe the operator
 * type in the final serialized string that gets pushed down.
 *
 * NOTE: the codes could be forced into a single byte, but list will
 * grow larger in the future, so why bother.
 */
typedef enum OperatorCode
{
	OP_LT = 1,
	OP_GT,
	OP_LE,
	OP_GE,
	OP_EQ,
	OP_NE,
	OP_LIKE,
	OP_IS_NULL,
	OP_IS_NOTNULL,
	OP_IN

} OperatorCode;

/*
 * each supported operand from both sides of the operator is represented
 * by a code that will describe the operator type in the final serialized
 * string that gets pushed down.
 */
#define ATTR_CODE				'a'
#define SCALAR_CONST_CODE		'c'
#define LIST_CONST_CODE			'm'
#define SIZE_BYTES				's'
#define CONST_DATA				'd'
#define OPERATOR_CODE			'o'
#define LOGICAL_OPERATOR_CODE	'l'

#define NullConstValue	 "NULL"
#define TrueConstValue	 "true"
#define FalseConstValue   "false"

/*
 * An Operand has any of the above codes, and the information specific to
 * its type. This could be compacted but filter structures are expected to
 * be very few and very small so it's ok as is.
 */
typedef struct Operand
{
	char		opcode;			/* ATTR_CODE, SCALAR_CONST_CODE,
								 * LIST_CONST_CODE */
	AttrNumber	attnum;			/* used when opcode is ATTR_CODE */
	StringInfo	conststr;		/* used when opcode is SCALAR_CONST_CODE
								 * or LIST_CONST_CODE */
	Oid			consttype;		/* used when opcode is SCALAR_CONST_CODE
								 * or LIST_CONST_CODE */
} Operand;

/*
 * A dlproxy filter has a left and right operands, and an operator.
 */
typedef struct FilterDesc
{
	Operand	l;				/* left operand */
	Operand	r;				/* right operand or InvalidAttrNumber if none */
	OperatorCode op;		/* operator code */
} FilterDesc;

/*
 * A mapping of GPDB operator OID to dlproxy operator code.
 * Used for Node of type 'T_OpExpr'
 */
typedef struct dbop_dlproxyop_map
{
	Oid			dbop;
	OperatorCode dlproxyop;
} dbop_dlproxyop_map;

/*
 * A mapping of GPDB operator OID to dlproxy operator code ('OP_IN').
 * Used for Node of type 'T_ScalarArrayOpExpr'
 */
typedef struct dbop_dlproxyop_array_map
{
	Oid			dbop;
	OperatorCode dlproxyop;
	bool		useOr;
} dbop_dlproxyop_array_map;

typedef struct ExpressionItem
{
	Node	   *node;
	Node	   *parent;
	bool		processed;
} ExpressionItem;

static inline bool
operand_is_attr(Operand x)
{
	return (x.opcode == ATTR_CODE);
}

static inline bool
operand_is_scalar_const(Operand x)
{
	return (x.opcode == SCALAR_CONST_CODE);
}

static inline bool
operand_is_list_const(Operand x)
{
	return (x.opcode == LIST_CONST_CODE);
}

char *serializeDlProxyFilterQuals(List *quals);
List *extractDlProxyAttributes(List *quals, bool *qualsAreSupported);

#endif   /* // _FILTERS_H_ */
