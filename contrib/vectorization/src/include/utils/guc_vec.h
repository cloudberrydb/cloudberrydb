/*-------------------------------------------------------------------------
 * guc_vec.h
 *	  Vectorization GUCs
 *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 *
 * IDENTIFICATION
 *		src/include/utils/guc_vec.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GUC_VEC_H
#define GUC_VEC_H

/* max vectorization count */
extern int max_batch_size;
/* deciding whether to enable vectorization */
extern bool enable_vectorization;
/* whether to force vectorization, and if neither orca nor non-ORCA 
supports vectors, use the optimizer originally set*/
extern bool force_vectorization;
/* min concatenate rows */
extern int min_concatenate_rows;
/* min redistribute motion handle rows */
extern int min_redistribute_handle_rows;
void assign_enable_vectorization(bool newval, void *extra);
/* merge some small arrow plans into a big one if true */
extern bool enable_arrow_plan_merge;

#endif   /* GUC_VEC_H */
