/*
 * Copyright (c) 2011 EMC Corporation All Rights Reserved
 *
 * This software is protected, without limitation, by copyright law
 * and international treaties. Use of this software and the intellectual
 * property contained therein is expressly limited to the terms and
 * conditions of the License Agreement under which it is provided by
 * or on behalf of EMC.
 *
 * ---------------------------------------------------------------------
 *
 * The quicklz implementation is not provided due to licensing issues.  The
 * following stub implementation is built if a proprietary implementation is
 * not provided.
 *
 * Note that other compression algorithms actually work better for new
 * installations anyway.
 */

#include "postgres.h"
#include "utils/builtins.h"

Datum
quicklz_constructor(PG_FUNCTION_ARGS)
{
	elog(ERROR, "quicklz compression not supported");
	PG_RETURN_VOID();
}

Datum
quicklz_destructor(PG_FUNCTION_ARGS)
{
	elog(ERROR, "quicklz compression not supported");
	PG_RETURN_VOID();
}

Datum
quicklz_compress(PG_FUNCTION_ARGS)
{
	elog(ERROR, "quicklz compression not supported");
	PG_RETURN_VOID();
}

Datum
quicklz_decompress(PG_FUNCTION_ARGS)
{
	elog(ERROR, "quicklz compression not supported");
	PG_RETURN_VOID();
}

Datum
quicklz_validator(PG_FUNCTION_ARGS)
{
	elog(ERROR, "quicklz compression not supported");
	PG_RETURN_VOID();
}


