//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		IMDRelation.h
//
//	@doc:
//		Interface for relations in the metadata cache
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPMD_IMDRelation_H
#define GPMD_IMDRelation_H

#include "gpos/base.h"

#include "naucrates/md/IMDCacheObject.h"
#include "naucrates/md/IMDColumn.h"
#include "naucrates/md/IMDPartConstraint.h"
#include "naucrates/statistics/IStatistics.h"

namespace gpdxl
{
	//fwd declaration
	class CXMLSerializer;
}

namespace gpmd
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		IMDRelation
	//
	//	@doc:
	//		Interface for relations in the metadata cache
	//
	//---------------------------------------------------------------------------
	class IMDRelation : public IMDCacheObject
	{		
		public:
			
			//-------------------------------------------------------------------
			//	@doc:
			//		Storage type of a relation
			//-------------------------------------------------------------------
			enum Erelstoragetype
			{
				ErelstorageHeap,
				ErelstorageAppendOnlyCols,
				ErelstorageAppendOnlyRows,
				ErelstorageAppendOnlyParquet,
				ErelstorageExternal,
				ErelstorageVirtual,
				ErelstorageSentinel
			};
			
			//-------------------------------------------------------------------
			//	@doc:
			//		Distribution policy of a relation
			//-------------------------------------------------------------------
			enum Ereldistrpolicy
			{
				EreldistrMasterOnly,
				EreldistrHash,
				EreldistrRandom,
				EreldistrSentinel
			};

		protected:

			// serialize an array of column ids into a comma-separated string
			static
			CWStringDynamic *PstrColumns(IMemoryPool *pmp, DrgPul *pdrgpul);

		public:
			
			// object type
			virtual
			Emdtype Emdt() const
			{
				return EmdtRel;
			}
			
			// is this a temp relation
			virtual 
			BOOL FTemporary() const = 0;
			
			// storage type (heap, appendonly, ...)
			virtual 
			Erelstoragetype Erelstorage() const = 0; 
			
			// distribution policy (none, hash, random)
			virtual 
			Ereldistrpolicy Ereldistribution() const = 0;
			
			// number of columns
			virtual 
			ULONG UlColumns() const = 0;
			
			// does relation have dropped columns
			virtual
			BOOL FHasDroppedColumns() const = 0; 
			
			// number of non-dropped columns
			virtual 
			ULONG UlNonDroppedCols() const = 0; 
			
			// return the position of the given attribute position excluding dropped columns
			virtual 
			ULONG UlPosNonDropped(ULONG ulPos) const = 0;
			
			 // return the position of a column in the metadata object given the attribute number in the system catalog
			virtual
			ULONG UlPosFromAttno(INT iAttno) const = 0;

			// return the original positions of all the non-dropped columns
			virtual
			DrgPul *PdrgpulNonDroppedCols() const = 0;

			// retrieve the column at the given position
			virtual 
			const IMDColumn *Pmdcol(ULONG ulPos) const = 0;
			
			// number of key sets
			virtual
			ULONG UlKeySets() const = 0;
			
			// key set at given position
			virtual
			const DrgPul *PdrgpulKeyset(ULONG ulPos) const = 0;
			
			// number of distribution columns
			virtual 
			ULONG UlDistrColumns() const = 0;
			
			// retrieve the column at the given position in the distribution key for the relation
			virtual 
			const IMDColumn *PmdcolDistrColumn(ULONG ulPos) const = 0;
			
			// return true if a hash distributed table needs to be considered as random
			virtual 
			BOOL FConvertHashToRandom() const = 0;
			
			// does this table have oids
			virtual
			BOOL FHasOids() const = 0;

			// is this a partitioned table
			virtual
			BOOL FPartitioned() const = 0;
			
			// number of partition columns
			virtual
			ULONG UlPartColumns() const = 0;
			
			// number of partitions
			virtual
			ULONG UlPartitions() const = 0;

			// retrieve the partition column at the given position
			virtual 
			const IMDColumn *PmdcolPartColumn(ULONG ulPos) const = 0;
			
			// number of indices
			virtual 
			ULONG UlIndices() const = 0;

			// number of triggers
			virtual
			ULONG UlTriggers() const = 0;

			// retrieve the id of the metadata cache index at the given position
			virtual 
			IMDId *PmdidIndex(ULONG ulPos) const = 0;

			// retrieve the id of the metadata cache trigger at the given position
			virtual
			IMDId *PmdidTrigger(ULONG ulPos) const = 0;

			// number of check constraints
			virtual
			ULONG UlCheckConstraints() const = 0;

			// retrieve the id of the check constraint cache at the given position
			virtual
			IMDId *PmdidCheckConstraint(ULONG ulPos) const = 0;

			// part constraint
			virtual
			IMDPartConstraint *Pmdpartcnstr() const = 0;
			
			// relation distribution policy as a string value
			static
			const CWStringConst *PstrDistrPolicy(Ereldistrpolicy ereldistrpolicy);
			
			// name of storage type
			static
			const CWStringConst *PstrStorageType(IMDRelation::Erelstoragetype erelstorage);
	};
	
}



#endif // !GPMD_IMDRelation_H

// EOF
