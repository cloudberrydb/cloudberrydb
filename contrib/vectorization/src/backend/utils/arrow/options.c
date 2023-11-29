/*-------------------------------------------------------------------------
 *
 * options.c
 *	  build arrow plan node options
 *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 * IDENTIFICATION
 *	  src/backend/utils/arrow/options.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/arrow.h"

static GArrowFunctionOptions *build_count_options(int numargs, GArrowCountMode mode);

GArrowFunctionOptions *
build_all_count_options(int numargs)
{
	GArrowCountMode mode;
	if (!numargs)
		mode = GARROW_COUNT_MODE_ALL;
	else
		mode = GARROW_COUNT_MODE_ONLY_VALID;
    return build_count_options(numargs, mode);
}

static GArrowFunctionOptions *
build_count_options(int numargs, GArrowCountMode mode)
{
	g_autoptr(GArrowCountOptions) count_options = NULL;
	count_options = garrow_count_options_new();
	garrow_count_options_set(count_options, mode);
	return (GArrowFunctionOptions *)garrow_move_ptr(count_options);
}
