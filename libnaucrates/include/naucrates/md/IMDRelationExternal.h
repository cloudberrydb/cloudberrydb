//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		IMDRelationExternal.h
//
//	@doc:
//		Interface for external relations in the metadata cache
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPMD_IMDRelationExternal_H
#define GPMD_IMDRelationExternal_H

#include "gpos/base.h"

#include "naucrates/md/IMDRelation.h"

namespace gpmd
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		IMDRelationExternal
	//
	//	@doc:
	//		Interface for external relations in the metadata cache
	//
	//---------------------------------------------------------------------------
	class IMDRelationExternal : public IMDRelation
	{
		public:

			// storage type
			virtual
			Erelstoragetype Erelstorage() const
			{
				return ErelstorageExternal;
			}

			// is this a temp relation
			virtual
			BOOL FTemporary() const
			{
				return false;
			}

			// is this a partitioned table
			virtual
			BOOL FPartitioned() const
			{
				return false;
			}
			
			// return true if a hash distributed table needs to be considered as random
			virtual
			BOOL FConvertHashToRandom() const = 0;
			
			// does this table have oids
			virtual
			BOOL FHasOids() const
			{
				return false;
			}

			// number of partition columns
			virtual
			ULONG UlPartColumns() const
			{
				return 0;
			}

			// number of partitions
			virtual
			ULONG UlPartitions() const
			{
				return 0;
			}

			// retrieve the partition column at the given position
			virtual
			const IMDColumn *PmdcolPartColumn(ULONG /*ulPos*/) const
			{
				GPOS_ASSERT(!"External tables have no partition columns");
				return NULL;
			}

			// part constraint
			virtual
			IMDPartConstraint *Pmdpartcnstr() const
			{
				return NULL;
			}

			// reject limit
			virtual
			INT IRejectLimit() const = 0;

			// reject limit in rows?
			virtual
			BOOL FRejLimitInRows() const = 0;

			// format error table mdid
			virtual
			IMDId *PmdidFmtErrRel() const = 0;

	};
}

#endif // !GPMD_IMDRelationExternal_H

// EOF
