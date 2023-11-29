/*--------------------------------------------------------------------
 * guc_vec.h
 *	  Define Vectorization GUCs
 *
 *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 * IDENTIFICATION
 *	  src/backend/misc/guc_vec.c
 *
 *--------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/guc_vec.h"
#include "cdb/cdbvars.h"
#include "miscadmin.h"

/* max vectorization count */
int max_batch_size = 0;

/* deciding whether to enable vectorization */
bool enable_vectorization = false;

bool force_vectorization = false;

int min_concatenate_rows = 0;
int min_redistribute_handle_rows = 0;

void
assign_enable_vectorization(bool newval, void *extra)
{
	if (newval == true)
	{
		Gp_interconnect_queue_depth = 64;
		Gp_interconnect_snd_queue_depth = 64;
	}
	else
	{
		Gp_interconnect_queue_depth = 4;
		Gp_interconnect_snd_queue_depth = 2;
	}
}
