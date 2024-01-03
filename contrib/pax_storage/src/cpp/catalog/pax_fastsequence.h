//-------------------------------------------------------------------------
// Cloudberry Database
// Copyright (c) 2023, HashData Technology Limited.
// pax_fastsequence.h
// provide a system table maintaining a light-weight fast sequence number for a
// unique object.
//
// IDENTIFICATION
//	    src/catalog/pax_fastsequence.h
// Author: Tony Ying
//--------------------------------------------------------------------------

#pragma once
#include "comm/cbdb_api.h"

#define ANUM_PG_PAX_FAST_SEQUENCE_OBJID 1
#define ANUM_PG_PAX_FAST_SEQUENCE_LASTSEQUENCE 2
#define NATTS_PG_PAX_FAST_SEQUENCE_TABLES 2

// CREATE:  initialize seqno by INSERT, no tuple exists before
// INPLACE: inplace update when grow the seqno or non-transactional truncate
// UPDATE:  transactional truncate, needs to preserve the old seqno
//          after rollback
#define FASTSEQUENCE_INIT_TYPE_CREATE 'C'
#define FASTSEQUENCE_INIT_TYPE_INPLACE 'I'
#define FASTSEQUENCE_INIT_TYPE_UPDATE 'U'

namespace paxc {
void CPaxInitializeFastSequenceEntry(Oid objid, char init_type);
int32 CPaxGetFastSequences(Oid objid);

}  // namespace paxc
