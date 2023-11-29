/*-------------------------------------------------------------------------
 *
 * veccdbhash.c
 *	  Implement hashing routines with arrow.
 *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 * IDENTIFICATION
 *	  src/backend/cdb/veccdbhash.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include  "cdb/veccdbhash.h"
#include "utils/fmgr_vec.h"
#include "utils/wrapper.h"

static void**
make_seg_scalars(int numLogicPart)
{
	GArrowScalarDatum **p = palloc0(sizeof(GArrowScalarDatum*) * numLogicPart);

	for (int i = 0; i < numLogicPart; i++)
	{
		g_autoptr(GArrowScalar) uint32_scalar = NULL;

		uint32_scalar = GARROW_SCALAR(DatumGetPointer(
				garrow_uint32_scalar_new(i)));

		p[i] = garrow_scalar_datum_new(uint32_scalar);
	}

	return (void**)p;
}

static int
ispowof2(int numsegs)
{
	return !(numsegs & (numsegs - 1));
}

VecCdbHash *
makeVecCdbHash(int numsegs, int natts, Oid *hashfuncs)
{
	VecCdbHash *h_vec;
	int i;
	bool is_legacy_hash = false;

	Assert(numsegs > 0);		/* verify number of segments is legal. */

	/* Allocate a new CdbHash, with space for the datatype OIDs. */
	h_vec = palloc(sizeof(VecCdbHash));
	h_vec->segmapping_scalar = make_seg_scalars(numsegs);
	h_vec->hasharray = NULL;
	CdbHash    *h = (CdbHash    *) h_vec;

	/*
	 * set this hash session characteristics.
	 */
	h->hash = 0;
	h->numsegs = numsegs;

	/* Load hash function info */
	h->hashfuncs = (FmgrInfo *) palloc(natts * sizeof(FmgrInfo));
	for (i = 0; i < natts; i++)
	{
		Oid			funcid = hashfuncs[i];

		if (isLegacyCdbHashFunction(funcid))
			is_legacy_hash = true;

		fmgr_info(funcid, &h->hashfuncs[i]);
	}
	h->natts = natts;
	h->is_legacy_hash = is_legacy_hash;

	/*
 	 * set the reduction algorithm: If num_segs is power of 2 use bit mask,
	 * else use lazy mod (h mod n)
	 */
	if (is_legacy_hash)
		h->reducealg = ispowof2(numsegs) ? REDUCE_BITMASK : REDUCE_LAZYMOD;
	else
		h->reducealg = REDUCE_JUMP_HASH;

	ereport(DEBUG4,
			(errmsg("CDBHASH hashing into %d segment databases", h->numsegs)));

	return h_vec;
}

static Datum
vec_fast_mode(void *hash, int32 num_segments)
{
	g_autoptr(GError)	error = NULL;
	GList		*args  = NULL;
	g_autoptr(GArrowFunction)  func = NULL;
	g_autoptr(GArrowArrayDatum)    result_array_datum = NULL;

	g_autoptr(GArrowScalar)         num_segments_scalar = NULL;
	g_autoptr(GArrowScalarDatum)    num_segments_scalar_datum = NULL;
	g_autoptr(GArrowArray)    uint32_array = NULL;
	g_autoptr(GArrowArrayDatum)     hash_array_datum = NULL;
	g_autoptr(GArrowDoubleArray)    double_cast_array = NULL;
	g_autoptr(GArrowDoubleDataType) double_cast_type = NULL;
	g_autoptr(GArrowDataType) dt ;
	g_autoptr(GArrowCastOptions)  cast_options = NULL;
	
	cast_options = garrow_cast_options_new();
	garrow_set_cast_options(cast_options, GARROW_PROP_ALLOW_INT_OVERFLOW, true);

	dt = GARROW_DATA_TYPE(garrow_uint32_data_type_new());
	uint32_array = garrow_array_cast(GARROW_ARRAY(hash), dt, cast_options, &error);
	if (error)
		elog(ERROR, "Fail to cast hash to uint32 array, cause: %s", error->message);	

	/* cast to double array */
	double_cast_type = GARROW_DOUBLE_DATA_TYPE(garrow_double_data_type_new());
	double_cast_array = GARROW_DOUBLE_ARRAY(
			garrow_array_cast(GARROW_ARRAY(uint32_array),
			GARROW_DATA_TYPE(double_cast_type),
			NULL,
			&error));
	hash_array_datum = GARROW_ARRAY_DATUM(garrow_array_datum_new(GARROW_ARRAY(double_cast_array)));

	/* prepare num_segments scalar */
	num_segments_scalar = GARROW_SCALAR(garrow_double_scalar_new(num_segments - 1));
	num_segments_scalar_datum = GARROW_SCALAR_DATUM(garrow_scalar_datum_new(num_segments_scalar));
    
	args = garrow_list_append_ptr(args, hash_array_datum);
	args = garrow_list_append_ptr(args, num_segments_scalar_datum);
   
	func = garrow_function_find("bit_wise_and");
	if (!func)
		 elog(ERROR, "bit_wise_and() not found in arrow");

	result_array_datum = GARROW_ARRAY_DATUM(
			 DatumGetPointer(garrow_function_execute(func, args, NULL, NULL, &error)));

	if (error)
		elog(ERROR, "vec_fast_mode arrow function error: %s", error->message);
     
	garrow_list_free_ptr(&args);
	PG_VEC_RETURN_POINTER(result_array_datum);
}


static Datum
vec_lazy_mode(void *hash, int32 num_segments)
{
	g_autoptr(GError)    error = NULL;
	GList     *args  = NULL;
	g_autoptr(GArrowFunction)      func = NULL;
	g_autoptr(GArrowArrayDatum)    result_array_datum = NULL;

	g_autoptr(GArrowScalar)         num_segments_scalar = NULL;
	g_autoptr(GArrowScalarDatum)    num_segments_scalar_datum = NULL;
	g_autoptr(GArrowArray)          uint32_array = NULL;
	g_autoptr(GArrowArrayDatum)     hash_array_datum = NULL;
	g_autoptr(GArrowDoubleArray)    double_cast_array = NULL;
	g_autoptr(GArrowDoubleDataType) double_cast_type = NULL;
	g_autoptr(GArrowDataType) dt ;
	g_autoptr(GArrowCastOptions)    cast_options = NULL;
	
	cast_options = garrow_cast_options_new();
	garrow_set_cast_options(cast_options, GARROW_PROP_ALLOW_INT_OVERFLOW, true);

	dt = GARROW_DATA_TYPE(garrow_uint32_data_type_new());
	uint32_array = garrow_array_cast(GARROW_ARRAY(hash), dt, cast_options, &error);
	if (error)
		elog(ERROR, "Fail to cast hash to uint32 array, cause: %s", error->message);	

	/* cast to double array */
	double_cast_type = GARROW_DOUBLE_DATA_TYPE(garrow_double_data_type_new());
	double_cast_array = GARROW_DOUBLE_ARRAY(
			garrow_array_cast(GARROW_ARRAY(uint32_array),
			GARROW_DATA_TYPE(double_cast_type),
			NULL,
			&error));
	hash_array_datum = GARROW_ARRAY_DATUM(garrow_array_datum_new(GARROW_ARRAY(double_cast_array)));

	/* prepare num_segments scalar */
	num_segments_scalar = GARROW_SCALAR(garrow_double_scalar_new(num_segments - 1));
	num_segments_scalar_datum = GARROW_SCALAR_DATUM(garrow_scalar_datum_new(num_segments_scalar));
    
	args = garrow_list_append_ptr(args, hash_array_datum);
	args = garrow_list_append_ptr(args, num_segments_scalar_datum);
   
	func = garrow_function_find("bit_wise_and");
	 if (!func)
		 elog(ERROR, "bit_wise_and() not found in arrow");

	result_array_datum = GARROW_ARRAY_DATUM(
			 DatumGetPointer(garrow_function_execute(func, args, NULL, NULL, &error)));

	if (error)
		elog(ERROR, "vec_fast_mode arrow function error: %s", error->message);
     
	garrow_list_free_ptr(&args);
	PG_VEC_RETURN_POINTER(result_array_datum);   
}

static Datum
vec_arrow_jump_consistent_hash(void *hash, int32 num_segments)
{
        GList    *args  = NULL;
        g_autoptr(GArrowScalarDatum)    num_segments_scalar_datum = NULL;
        g_autoptr(GArrowScalar)         num_segments_scalar = NULL;
        g_autoptr(GArrowArrayDatum)     hash_array_datum = NULL;
        g_autoptr(GArrowArrayDatum)     result_array_datum = NULL;
        g_autoptr(GArrowFunction)      func = NULL;
        g_autoptr(GError) error = NULL;
        g_autoptr(GArrowArray) array = NULL;
        g_autoptr(GArrowArray) uint32_array = NULL;

        g_autoptr(GArrowDataType) dt = NULL;
        g_autoptr(GArrowCastOptions)    cast_options = NULL;
        cast_options = garrow_cast_options_new();
        garrow_set_cast_options(cast_options, GARROW_PROP_ALLOW_INT_OVERFLOW, true);

        dt = GARROW_DATA_TYPE(garrow_uint32_data_type_new());
        uint32_array = garrow_array_cast(GARROW_ARRAY(hash), dt, cast_options, &error);
        if (error)
                elog(ERROR, "Fail to cast hash to uint32 array, cause: %s", error->message);


        hash_array_datum = GARROW_ARRAY_DATUM(garrow_array_datum_new(GARROW_ARRAY(uint32_array)));

        /* prepare num_segments scalar */
        num_segments_scalar = GARROW_SCALAR(garrow_uint32_scalar_new(num_segments));
        num_segments_scalar_datum = GARROW_SCALAR_DATUM(garrow_scalar_datum_new(num_segments_scalar));

        args = garrow_list_append_ptr(args, hash_array_datum);
        args = garrow_list_append_ptr(args, num_segments_scalar_datum);

        func = garrow_function_find("conHash");
        if (!func)
                elog(ERROR, "garrow_function_find(conHash) not found in arrow");

        result_array_datum = GARROW_ARRAY_DATUM(
                                                DatumGetPointer(garrow_function_execute(func, args, NULL, NULL, &error)));

        if (error)
                elog(ERROR, "vec_arrow_jump_consistent_hash arrow function error: %s", error->message);

	garrow_list_free_ptr(&args);
        PG_VEC_RETURN_POINTER(result_array_datum);
}

/*
 * Reduce the hash array to a segment number array
 */
Datum
cdbhashreduce_vec(VecCdbHash *h)
{
	g_autoptr(GArrowArray)      lgp_array = NULL;
	g_autoptr(GArrowArrayDatum) res_datum = NULL;
	g_autoptr(GArrowFunction)   func = NULL;

	GList         *args = NULL;
	g_autoptr(GError)        error = NULL;
	int           index = 0;

	/*
	 * Reduce our 32-bit hash value to a segment number
	 */
	switch (h->base.reducealg)
	{
		/*
		 * FIXME:bitmask and lazy mode have not been tested.
		 * jump_hash: while statement cannot be vectorized. This function is currently implemented in a non-vectorized manner
		 */
		case REDUCE_BITMASK:
			garrow_store_func(res_datum, vec_fast_mode(h->hasharray, h->base.numsegs));
			break;

		case REDUCE_LAZYMOD:
			garrow_store_func(res_datum,
					vec_lazy_mode(h->hasharray, h->base.numsegs));
			break;

		case REDUCE_JUMP_HASH:
			garrow_store_func(res_datum,
					vec_arrow_jump_consistent_hash(h->hasharray, h->base.numsegs));
			break;

		default:
			elog(ERROR, "not support reduce algorithm %d", h->base.reducealg);
			break;
	}

	func = garrow_function_find("choose");

	if (func == NULL)
	{
		elog(ERROR, "%s find choose function fail", __FUNCTION__);
	}

	args = garrow_list_append_ptr(args, res_datum);

	for (index = 0; index < h->base.numsegs; index++)
	{
		g_autoptr(GArrowDatum) tmp = garrow_copy_ptr(((VecCdbHash *)h)->segmapping_scalar[index]);
		args = garrow_list_append_ptr(args, tmp);
	}

	garrow_store_func(res_datum, garrow_function_execute(func, args, NULL, NULL, &error));

	if (error != NULL)
	{
		elog(ERROR, "%s execute choose function fail caused by %s", __FUNCTION__, error->message);
	}

	garrow_list_free_ptr(&args);

	PG_VEC_RETURN_POINTER(res_datum);
}

/*
 * get route target array for tuples
 */
Datum
evalHashKeyVec(void *projector, void *recordbatch, VecCdbHash *h, int rows)
{
	g_autoptr(GArrowUInt32Array) reconstructed_array = NULL;
	g_autoptr(GArrowUInt32Array) reconstructing_array = NULL;
	g_autoptr(GArrowArray)       seg_array = NULL;
	g_autoptr(GArrowBuffer)      buffer_from = NULL;
	g_autoptr(GArrowBuffer)      buffer_to = NULL;

	const   guint32 * dat = NULL;
	Datum target_seg = (Datum)0;
	g_autoptr(GArrowArray) array = NULL;
	GList   *rs = NULL;
	GError  *error = NULL;
	gint64  nulls = 0;
	gint64  size = 0;

	/*
 	 * 	If we have more than one distribution keys for this relation, hash them.
 	 * 	However, If this happens to be a relation with an empty policy
 	 * 	partitioning policy with a NULL distribution key list) then we have no
 	 * 	hash key value to feed in, so use cdbhashrandomseg_vec() to pick a segment
 	 *   at random.
 	 */

	rs = ggandiva_projector_evaluate(projector, recordbatch, &error);
	if (error)
		elog(ERROR, "Failed to evaluate hash32 projector, cause: %s", error->message);

	array = garrow_list_nth_data(rs, 0);

	garrow_store_ptr(h->hasharray, array);

	target_seg = cdbhashreduce_vec(h);

/* NOTICE: the origin array may contain null rows, these rows
	* would be set to zero, but also be set invalid, so should reconstruct
	* the array to erase invalid 
	*/
	seg_array = garrow_datum_get_array(GARROW_ARRAY_DATUM(DatumGetPointer(target_seg)));
	nulls = garrow_array_get_n_nulls(seg_array);
	if (nulls > 0)
	{
		dat = garrow_uint32_array_get_values(GARROW_UINT32_ARRAY(seg_array), &size);
		buffer_from = garrow_buffer_new((const guint8*)dat, size * sizeof(uint32));
		buffer_to = garrow_buffer_copy(buffer_from,
										0,
										size * sizeof(uint32),
										&error);

		if (error)
		{
			elog(ERROR, "%s copy data buffer fail caused by %s",
					__FUNCTION__, error->message);
		}

		reconstructed_array = garrow_uint32_array_new(size,
													buffer_to,
													NULL,
													0);
		free_array_datum((void**)DatumGetPointer(target_seg));

		target_seg = PointerGetDatum(garrow_array_datum_new(GARROW_ARRAY(reconstructed_array)));
	}
	garrow_list_free_ptr(&rs);
	return target_seg;
}
