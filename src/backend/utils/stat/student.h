/*-------------------------------------------------------------------------
 *
 * student.h
 *
 * Evaluate the Student-T distribution function.
 *
 *------------------------------------------------------------------------- 
 */

#ifndef STUDENT_H
#define STUDENT_H

#include "postgres.h"
#include "fmgr.h"

extern float8 studentT_cdf(uint64 /* nu */, float8 /* t */);

#endif
