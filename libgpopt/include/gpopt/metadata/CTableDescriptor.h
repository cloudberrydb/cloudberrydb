//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CTableDescriptor.h
//
//	@doc:
//		Abstraction of metadata for tables; represents metadata as stored
//		in the catalog -- not as used in queries, e.g. no aliasing etc.
//---------------------------------------------------------------------------
#ifndef GPOPT_CTableDescriptor_H
#define GPOPT_CTableDescriptor_H

#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/common/CBitSet.h"

#include "naucrates/md/IMDId.h"
#include "naucrates/md/CMDRelationGPDB.h"

#include "gpopt/base/CColRef.h"
#include "gpopt/metadata/CColumnDescriptor.h"

namespace gpopt
{
	using namespace gpos;	
	using namespace gpmd;
	
	// dynamic array of columns -- array owns columns
	typedef CDynamicPtrArray<CColumnDescriptor, CleanupRelease> DrgPcoldesc;
	
	// dynamic array of bitsets
	typedef CDynamicPtrArray<CBitSet, CleanupRelease> DrgPbs;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CTableDescriptor
	//
	//	@doc:
	//		metadata abstraction for tables
	//
	//---------------------------------------------------------------------------
	class CTableDescriptor : public CRefCount
	{
		private:
			
			// memory pool
			IMemoryPool *m_pmp;
			
			// mdid of the table
			IMDId *m_pmdid;
			
			// name of table
			CName m_name;
			
			// array of columns
			DrgPcoldesc *m_pdrgpcoldesc;
			
			// distribution policy
			IMDRelation::Ereldistrpolicy m_ereldistrpolicy;
			
			// storage type
			IMDRelation::Erelstoragetype m_erelstoragetype;

			// distribution columns for hash distribution
			DrgPcoldesc *m_pdrgpcoldescDist;
						
			// if true, we need to consider a hash distributed table as random
			// there are two possible scenarios:
			// 1. in hawq 2.0, some hash distributed tables need to be considered as random,
			//	  depending on its bucket number
			// 2. for a partitioned table, it may contain a part with a different distribution
			BOOL m_fConvertHashToRandom;
			
			// indexes of partition columns for partitioned tables
			DrgPul *m_pdrgpulPart;
			
			// key sets
			DrgPbs *m_pdrgpbsKeys;
			
			// number of leaf partitions
			ULONG m_ulPartitions;

			// id of user the table needs to be accessed with
			ULONG m_ulExecuteAsUser;

			// if true, it means this descriptor has partial indexes
			BOOL m_fHasPartialIndexes;

			// private copy ctor
			CTableDescriptor(const CTableDescriptor &);

			// returns true if this table descriptor has partial indexes
			BOOL FDescriptorWithPartialIndexes();

		public:

			// ctor
			CTableDescriptor
				(
				IMemoryPool *,
				IMDId *pmdid,
				const CName &,
				BOOL fConvertHashToRandom,
				IMDRelation::Ereldistrpolicy ereldistrpolicy,
				IMDRelation::Erelstoragetype erelstoragetype,
				ULONG ulExecuteAsUser
				);

			// dtor
			virtual
			~CTableDescriptor();

			// add a column to the table descriptor
			void AddColumn(CColumnDescriptor *);
			
			// add the column at the specified position to the list of distribution columns
			void AddDistributionColumn(ULONG ulPos);

			// add the column at the specified position to the list of partition columns
			void AddPartitionColumn(ULONG ulPos);

			// add a keyset
			BOOL FAddKeySet(CBitSet *pbs);
			
			// accessors
			ULONG UlColumns() const;
			const CColumnDescriptor *Pcoldesc(ULONG) const;
			
			// mdid accessor
			IMDId *Pmdid() const
			{
				return m_pmdid;
			}
			
			// name accessor
			const CName &Name() const
			{
				return m_name;
			}
			
			// execute as user accessor
			ULONG UlExecuteAsUser() const
			{
				return m_ulExecuteAsUser;
			}
			
			// return the position of a particular attribute (identified by attno)
			ULONG UlPosition(INT iAttno) const;

			// column descriptor accessor
			DrgPcoldesc *Pdrgpcoldesc() const
			{
				return m_pdrgpcoldesc;
			}
			
			// distribution column descriptors accessor
			const DrgPcoldesc *PdrgpcoldescDist() const
			{
				return m_pdrgpcoldescDist;
			}
			
			// partition column indexes accessor
			const DrgPul *PdrgpulPart() const
			{
				return m_pdrgpulPart;
			}
			
			// array of key sets
			const DrgPbs *PdrgpbsKeys() const
			{
				return m_pdrgpbsKeys;
			}

			// return the number of leaf partitions
			ULONG UlPartitions() const;

			// distribution policy
			IMDRelation::Ereldistrpolicy Ereldistribution() const 
			{
				return m_ereldistrpolicy;
			}
			
			// storage type
			IMDRelation::Erelstoragetype Erelstorage() const
			{
				return m_erelstoragetype;
			}

			BOOL FPartitioned() const
			{
				return 0 < m_pdrgpulPart->UlLength();
			}

			// true iff a hash distributed table needs to be considered as random
			BOOL FConvertHashToRandom() const
			{
				return m_fConvertHashToRandom;
			}
			
			// helper function for finding the index of a column descriptor in
			// an array of column descriptors
			ULONG UlPos(const CColumnDescriptor *, const DrgPcoldesc *) const;
			
#ifdef GPOS_DEBUG
			IOstream &OsPrint(IOstream &) const;
#endif // GPOS_DEBUG

			// returns number of indices
			ULONG UlIndices();

			// true iff this table has partial indexes
			BOOL FHasPartialIndexes() const
			{
				return m_fHasPartialIndexes;
			}

	}; // class CTableDescriptor
}

#endif // !GPOPT_CTableDescriptor_H

// EOF
