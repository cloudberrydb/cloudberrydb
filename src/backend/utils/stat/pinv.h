/* -----------------------------------------------------------------------------
 *
 * pinv.h
 *
 * @brief Compute the Moore-Penrose pseudo-inverse of a matrix.
 *
 * -------------------------------------------------------------------------- */

#ifndef PINV_H
#define PINV_H

#include "postgres.h"

extern void pinv(int /* rows */, int /* columns */,
    double * /* A */, double * /* Aplus */);

#endif
