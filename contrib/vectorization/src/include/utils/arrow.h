/*--------------------------------------------------------------------
 * arrow.h
 *	  Fundamental arrow definitions.

 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 * IDENTIFICATION
 *	  src/utils/arrow.h
 *
 *--------------------------------------------------------------------
 */
#ifndef ARROW_H
#define ARROW_H

#include <arrow-glib/arrow-glib.h>
#include <gandiva-glib/gandiva-glib.h>

#include "access/xact.h"
#include "nodes/execnodes.h"
#include "tuptable_vec.h"
#include "utils/builtins.h"
#include "utils/guc_vec.h"
#include "arrow/c/abi.h"

/* join prefix */
extern const char *LEFT_PREFIX;
extern const char *RIGHT_PREFIX;
#define CTID_FIELD "ctid"

/*
 * free a list of GArrow Objects.
 * e.g.:
 * garrow_list_free(GArrowArray, glist);
 */
static inline void garrow_list_free_ptr(GList **PL)
{
	GList* __node;
	for (__node = *(PL); __node; __node = g_list_next(__node))
	{
		if (__node->data)
			glib_autoptr_cleanup_GObject((GObject **)&__node->data);
	}
	g_list_free(*(PL));
	*(PL) = NULL;
}


/*
 * Append or prepend GArrowXxx auto pointer to GList.
 * Need to call garrow_list_free_ptr to free the pointers in list.
 *
 * C must be defined by g_autoptr(GArrowXxx).
 */
#define garrow_list_append_ptr(L, C) \
	garrow_list_append_pp((L), (&C));
#define garrow_list_prepend_ptr(L, C) \
	garrow_list_prepend_pp((L), (&C));

/* pdata is GArrowXxx** type. */
static inline G_GNUC_WARN_UNUSED_RESULT
GList* garrow_list_append_pp(GList *list, gpointer pdata)
{
	return g_list_append(list, g_steal_pointer((gpointer *)pdata));
}

static inline G_GNUC_WARN_UNUSED_RESULT
	GList* garrow_list_prepend_pp(GList *list, gpointer pdata)
{
	return g_list_prepend(list, g_steal_pointer((gpointer *)pdata));
}

static inline G_GNUC_WARN_UNUSED_RESULT
	gpointer garrow_list_nth_data(GList *list, guint n)
{
	return g_object_ref(g_list_nth_data(list, n));
}
#ifndef GARROW_DISABLE_DEPRECATED
G_DEPRECATED_FOR(garrow_list_free_ptr) void g_list_free(GList *list);
G_DEPRECATED_FOR(garrow_list_append_ptr) GList* g_list_append(GList *list, gpointer data);
G_DEPRECATED_FOR(garrow_list_preppend_ptr) GList* g_list_prepend(GList *list, gpointer data);
G_DEPRECATED_FOR(garrow_list_nth_data) gpointer g_list_nth_data(GList *list, guint n);
G_DEPRECATED_FOR(garrow_list_nth_data) GList*  g_list_nth(GList *list, guint n);
#endif

/*
 * store GArrowXxx pointer SRC to struct member DST.
 *
 * DST will be release if contains valid GArrowXxx pointer.
 * Notice that SRC pointer will be stolen, cannot be release.
 */
#define garrow_store_ptr(DST, SRC) 		\
	do { 								\
		void **D = (void **) &(DST); 	\
		if (DST) 						\
		{ 								\
			g_clear_object(D); 			\
		} 								\
		(*D) = g_steal_pointer(&(SRC)); 	\
	} while (0)

#define garrow_store_func(store, func_call) \
do { \
	g_autoptr(GObject) RETURNDATUM = G_OBJECT((func_call)); \
	garrow_store_ptr(store, RETURNDATUM); \
}while(0);

/*
 * Free Arrow type.
 *
 * If *PP is NULL, skip it. Always set *PP to NULL.
 *
 * T: the type, eg: GArrowArray
 * P: the T-type pointer.
 */
#define ARROW_FREE(T, PP) 							\
	do { 											\
		if (*(PP)) 									\
		{ 											\
			glib_autoptr_cleanup_##T ((T **)(PP)); 	\
			*((T **)(PP)) = NULL; 					\
		} 											\
	} while (0)

/* Free GArrowRecordBatch* P */
#define ARROW_FREE_BATCH(P) ARROW_FREE(GArrowRecordBatch, P)

/*
 * get GArrowXxx pointer form struct member P.
 *
 * Return value must be g_autoptr(GArrowXxx) type.
 * This function get a reference of P, caller shoule free
 * the return pointer.
 */
#define garrow_copy_ptr(P) g_object_ref(P)

/* Used for return value or owner transfer. */
#define garrow_move_ptr(P) g_steal_pointer(&(P))

/*
 * Get the GArrowXxx* autoptr from function argument.
 *
 * Usually, called by Arrow type conversion. e.g.:
 * func(arg)
 * {
 * 	   g_autoptr(GArowXxx) argptr = GARROWXXX(garrow_get_argument(arg));
 * }
 */
#define garrow_get_argument(P) g_object_ref(P)

/*
* Some transform functions
*/
extern GArrowDataType *PGTypeToArrow(Oid pg_type);
extern GArrowType PGTypeToArrowID(Oid pg_type);
extern Datum GBytesToDatum(GBytes *bytes);

/*
 * Some vector related structs
 */

#define BASIC_COL_DATA_BUFF_SIZE (1024 * 1024 * 2)
#define BASIC_COL_OFFSET_BUFF_SIZE ((max_batch_size + 1) * sizeof(uint32))

typedef struct ColDesc
{
	void *values; /* builder for variable-size data, raw buff for fixed-size. */
	GArrowBuffer *value_buffer;
	uint8 *bitmap;
	GArrowBuffer *bitmap_buffer;
	void *offsets; /* data offsets for string or binary type */
	GArrowBuffer *offset_buffer;
	bool hasnull; /* always false for variable-sized data, in this case nulls contains in builder.*/
	GArrowType type;
	int datalen; /* size in Byte of each data, -1 for variable-size data and boolean */
	bool isbpchar;
	int currows;
	int rowstoread;
	int values_len;
	int cur_pos_index;
	bool is_lm;    /* the column is whether for late materialize */
	bool is_pos_recorder; /* the column is whether to record scanned row number */
	GArrowBuffer *pos_value_buffer; /* array buffer for scanned row number */
	void *pos_values;   /* buffer for scanned row number */
	int lm_row_index;
	int block_row_count;
} ColDesc;

/*
 * VecDesc
 *
 * Descriptor for a VecValue
 */
typedef struct vecDesc
{
	uint64 cur_ctid;
	int32 natts;
	int32 rows;
	GArrowArray *selective;

	/* visimap information*/
	List *visbitmaps; /* list of visimapEntry */
	int64 firstrow; /* the first row number of visbitmaps*/
	int64 boundrow; /* the bound row number of visbitmaps*/
	int64 visbitmapoffset; /* offset in visibility bitmap for current row.
							  Start from 1 */
	bool scan_ctid;

	GArrowSchema *schema;
	ColDesc **coldescs;

	struct ArrowRecordBatch abi_batch;
} *VecDesc;

typedef struct resDesc
{
	bool is_send;
	int32 natts;
	int32 rows;
	GArrowArray *selective_pos;
	GArrowArray **cols_array;
	GList *lst_pos;         /* list for position array */
	GList **lst_col;         /* list for column data array */
} *ResDesc;

static inline void
AppendDatumToColDesc(ColDesc *coldesc, void *datump)
{
	/* clear null bitmap for first append. */
	if ((coldesc->currows == 0) && (coldesc->hasnull))
	{
		memset(coldesc->bitmap, 0xff, (max_batch_size + 7) / 8);
		coldesc->hasnull = false;
	}

	/*
	 * Performance is so critical we don't use a switch statement here.
	 */
	if (coldesc->datalen == 1)
	{
		*(((uint8 *)coldesc->values) + coldesc->currows) = *(uint8 *) datump;
	}
	else if (coldesc->datalen == 2)
	{
		*(((uint16 *)coldesc->values) + coldesc->currows) = *(uint16 *)datump;
	}
	else if (coldesc->datalen == 4)
	{
		*(((uint32 *)coldesc->values) + coldesc->currows) = *(uint32 *) datump;
	}
	else if (coldesc->datalen == 8)
	{
		*(((Datum *)coldesc->values) + coldesc->currows) = *(Datum *) datump;
	}
	else if (coldesc->datalen == -1)
	{
		GError *error = NULL;

		if (coldesc->type == GARROW_TYPE_BOOLEAN)
		{
			if (*(bool *) datump)
			{
				*((uint8 *)coldesc->values + coldesc->currows / 8) |=
						((uint8)1) << coldesc->currows % 8;
			}
		}
		else if (coldesc->type == GARROW_TYPE_STRING)
		{
			if (!garrow_string_array_builder_append_string((GArrowStringArrayBuilder *)coldesc->values,
					(gchar*)datump, &error))
				elog(ERROR, "append %s as string failed: %s", (gchar*)datump, error->message);
		}
		else
			elog(ERROR, "invalid type to append: %d", coldesc->type);
	}
	else
	{
		Assert(false);
	}

	coldesc->currows++;
}

static inline void
AppendNullToColDesc(ColDesc *coldesc)
{

	/* clear null bitmap for first append. */
	if (coldesc->currows == 0)
	{
		memset(coldesc->bitmap, 0xff, (max_batch_size + 7) / 8);
		coldesc->hasnull = false;
	}

	int dpos = coldesc->currows;
	uint8 *dbit = coldesc->bitmap + dpos / 8;

	*dbit &= ~(1<<(dpos % 8));
	coldesc->hasnull = true;

    if (coldesc->datalen < 0 && coldesc->type != GARROW_TYPE_BOOLEAN)
    		((int*)(coldesc->offsets))[coldesc->currows+1] = ((int*)(coldesc->offsets))[coldesc->currows];

	coldesc->currows++;
}

static inline const char *
GetSchemaName(GArrowSchema *schema, AttrNumber attr, int *map)
{
	g_autoptr(GArrowField) field = NULL;

	if (!map)
		field = garrow_schema_get_field(schema, attr - 1);
	else
		field = garrow_schema_get_field(schema, map[attr - 1]);
	return garrow_field_get_name(field);
}

static inline const char *
GetCtidSchemaName(GArrowSchema *schema)
{
	const char *name = NULL;
	GList *fields = NULL;
	fields = garrow_schema_get_fields(schema);
	int n_fields = garrow_schema_n_fields(schema);
	for (int i = 0; i < n_fields; i++)
	{
		g_autoptr(GArrowField) field = NULL;
		field = garrow_schema_get_field(schema, i);
		name = garrow_field_get_name(field);
		if (strstr(name, CTID_FIELD))
			break;
	}
	return name;
}

static inline const char *
GetSchemaNameByVarNo(GArrowSchema *schema, AttrNumber attr, int varno)
{
	const char *name = NULL;
	GList *fields = NULL;
	bool find = false;
	fields = garrow_schema_get_fields(schema);
	int n_fields = garrow_schema_n_fields(schema);
	char *att_name = (char *)palloc(NAMEDATALEN);
	snprintf(att_name, NAMEDATALEN, "att%d", attr);
	for (int i = 0; i < n_fields; i++)
	{
		g_autoptr(GArrowField) field = NULL;
		field = garrow_schema_get_field(schema, i);
		name = garrow_field_get_name(field);
		if (varno == OUTER_VAR &&
			strstr(name, LEFT_PREFIX) && strstr(name, att_name))
			find = true;
		else if (varno == INNER_VAR &&
			strstr(name, RIGHT_PREFIX) && strstr(name, att_name))
			find = true;
		if (find)
			break;
	}
	pfree(att_name);
	garrow_list_free_ptr(&fields);
	return find ? name : NULL;
}

extern void ResetColDesc(ColDesc *coldesc);
extern void *FinishPosColDesc(ColDesc *coldesc);
extern void* FinishColDesc(ColDesc *coldesc);
extern void FreeVecDesc(VecDesc vecdesc);
extern void FreeResDesc(ResDesc resdesc);
extern void FreeVecSchema(TupleTableSlot *slot);
extern void PrintArrowDatum(void *datum, const char *label);
extern void PrintArrowArray(void *array, const char *label);

/* schema converters */
extern VecDesc TupleDescToVecDesc(TupleDesc tupdesc);
extern VecDesc TupleDescToVecDescLM(TupleDesc tupdesc,
									AttrNumber *valid_attrs,
									int nvalid,
									AttrNumber *lm_attrs,
									int nlm);
extern ResDesc TupleDescToResDescLM(TupleDesc tupdesc);

extern void resetVecDescBitmap(VecDesc vecdes);

/* arrow plan options */
extern GArrowFunctionOptions *build_all_count_options(int numargs);
extern GArrowFunctionOptions *build_all_sum_options(int numargs);

/* debug */
extern void DebugArrowPlan(GArrowExecutePlan *plan, const char *label);

extern GGandivaNode *
ExprToNode(ExprState *exprstate, Expr *expr, GArrowSchema *schema, GArrowDataType **rettype);

/* consistency hash */
extern GGandivaNode*
make_bitwise_or_fnode(GGandivaNode *node1,  GGandivaNode *node2);

extern GGandivaNode*
make_right_shift_fnode(GGandivaNode *node,  GGandivaNode *rnode);

extern GGandivaNode*
make_left_shift_fnode(GGandivaNode *node,  GGandivaNode *lnode);

extern GGandivaNode*
make_int32_literal_fnode(int32 val);

extern GGandivaNode*
make_xor_fnode(GGandivaNode *node1, GGandivaNode *node2);

extern GGandivaNode*
make_hash32_fnode(GGandivaNode *node);

extern GGandivaNode*
make_cdbhash32_fnode(GGandivaNode *node_first, GGandivaNode *node_second);

extern GGandivaNode*
make_result_cols_hash_fnode(GArrowSchema *schema, List *hash_keys);

/*
 * ArrowInt128 varlen data
 */
typedef struct Int128
{
	int32		 vl_len_;		/* varlena header (do not touch directly!) */
	GArrowInt128 *data;
} Int128;


extern Datum int128_in(PG_FUNCTION_ARGS);
extern Datum int128_out(PG_FUNCTION_ARGS);
extern Oid INT128OID;
extern Oid STDDEVOID;
extern Oid AvgIntByteAOid;
extern void init_vector_types(void);
extern const char *nodeTypeToString(const void *obj);

/*
 * global dummy schema
 */
extern GArrowSchema *dummy_schema;
extern void FreeDummySchema(void);
extern GArrowSchema *GetDummySchema(void);
extern void dummy_schema_xact_cb(XactEvent event, void *arg);
#endif   /* arrow */
