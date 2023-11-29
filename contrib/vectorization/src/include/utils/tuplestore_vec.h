#ifndef VEC_TUPLESTORE_H
#define VEC_TUPLESTORE_H

#include "postgres.h"

#include <limits.h>

#include "access/htup_details.h"
#include "commands/tablespace.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "storage/buffile.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

#include "cdb/cdbvars.h"
#include "executor/instrument.h"        /* struct Instrumentation */
#include "utils/workfile_mgr.h"
#include "utils/arrow.h"
#include "vecexecutor/execslot.h"
#include "utils/vecfuncs.h"

extern Tuplestorestate *tuplestore_begin_batch(bool randomAccess,
                                               bool interXact,
                                               int maxKBytes);
extern void tuplestore_putvecslot(Tuplestorestate *state,
                                  TupleTableSlot *slot);
extern Tuplestorestate *tuplestore_open_shared_vec(SharedFileSet *fileset, const char *filename);
extern bool tuplestore_getvecslot(Tuplestorestate *state, bool forward,
                                  bool copy, TupleTableSlot *slot);
extern bool tuplestore_advance_vec(Tuplestorestate *state, bool forward);
extern void tuplestore_end_vec(Tuplestorestate *state);
extern void tuplestore_trim_vec(Tuplestorestate *state);

#endif   /* VEC_TUPLESTORE_H */