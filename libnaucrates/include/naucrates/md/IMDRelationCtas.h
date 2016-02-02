//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		IMDRelationCtas.h
//
//	@doc:
//		Interface for CTAS relation entries in the metadata cache
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPMD_IMDRelationCTAS_H
#define GPMD_IMDRelationCTAS_H

#include "gpos/base.h"

#include "naucrates/md/IMDRelation.h"

namespace gpdxl
{
	// fwd decl
	class CDXLCtasStorageOptions;
}

namespace gpmd
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		IMDRelationCtas
	//
	//	@doc:
	//		Interface for CTAS relation entries in the metadata cache
	//
	//---------------------------------------------------------------------------
	class IMDRelationCtas : public IMDRelation
	{
		public:

			// schema name
			virtual
			CMDName *PmdnameSchema() const = 0;
			
			// is this a partitioned table
			virtual
			BOOL FPartitioned() const
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
			const IMDColumn *PmdcolPartColumn
				(
				ULONG // ulPos
				) 
				const
			{
				GPOS_ASSERT(!"CTAS tables have no partition columns");
				return NULL;
			}
			
			// return true if a hash distributed table needs to be considered as random
			virtual
			BOOL FConvertHashToRandom() const
			{
				return false;
			}
			
			// returns the number of key sets
			virtual
			ULONG UlKeySets() const
			{	
				return 0;
			}

			// returns the key set at the specified position
			virtual
			const DrgPul *PdrgpulKeyset
				(
				ULONG // ulPos
				) 
				const
			{	
				GPOS_ASSERT(!"CTAS tables have no keys");
				return NULL;
			}

			// part constraint
			virtual
			IMDPartConstraint *Pmdpartcnstr() const
			{
				return NULL;
			}

			// CTAS storage options
			virtual 
			CDXLCtasStorageOptions *Pdxlctasopt() const = 0;

	};
}

#endif // !GPMD_IMDRelationCTAS_H

// EOF
