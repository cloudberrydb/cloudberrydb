//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMDRelationGPDB.h
//
//	@doc:
//		Class representing MD relations
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------



#ifndef GPMD_CMDRelationGPDB_H
#define GPMD_CMDRelationGPDB_H

#include "gpos/base.h"
#include "gpos/string/CWStringDynamic.h"

#include "naucrates/md/IMDRelation.h"
#include "naucrates/md/IMDColumn.h"

#include "naucrates/md/CMDColumn.h"
#include "naucrates/md/CMDName.h"

namespace gpdxl
{
	class CXMLSerializer;
}

namespace gpmd
{
	using namespace gpos;
	using namespace gpdxl;

	//---------------------------------------------------------------------------
	//	@class:
	//		CMDRelationGPDB
	//
	//	@doc:
	//		Class representing MD relations
	//
	//---------------------------------------------------------------------------
	class CMDRelationGPDB : public IMDRelation
	{		
		private:
			// memory pool
			IMemoryPool *m_pmp;

			// DXL for object
			const CWStringDynamic *m_pstr;
			
			// relation mdid
			IMDId *m_pmdid;
			
			// table name
			CMDName *m_pmdname;
			
			// is this a temporary relation
			BOOL m_fTemporary;
			
			// storage type
			Erelstoragetype m_erelstorage;
			
			// distribution policy
			Ereldistrpolicy m_ereldistrpolicy;
			
			// columns
			DrgPmdcol *m_pdrgpmdcol;
			
			// number of dropped columns
			ULONG m_ulDroppedCols;
			
			// indices of distribution columns
			DrgPul *m_pdrgpulDistrColumns;
			
			// do we need to consider a hash distributed table as random distributed
			BOOL m_fConvertHashToRandom;

			// indices of partition columns
			DrgPul *m_pdrgpulPartColumns;
			
			// number of partition
			ULONG m_ulPartitions;

			// array of key sets
			DrgPdrgPul *m_pdrgpdrgpulKeys;

			// array of index ids
			DrgPmdid *m_pdrgpmdidIndices;

			// array of trigger ids
			DrgPmdid *m_pdrgpmdidTriggers;

			// array of check constraint mdids
			DrgPmdid *m_pdrgpmdidCheckConstraint;

			// partition constraint
			IMDPartConstraint *m_pmdpartcnstr;

			// does this table have oids
			BOOL m_fHasOids;

			// mapping of column position to positions excluding dropped columns
			HMUlUl *m_phmululNonDroppedCols;
		
			// mapping of attribute number in the system catalog to the positions of
			// the non dropped column in the metadata object
			HMIUl *m_phmiulAttno2Pos;

			// the original positions of all the non-dropped columns
			DrgPul *m_pdrgpulNonDroppedCols;
			
			// private copy ctor
			CMDRelationGPDB(const CMDRelationGPDB &);
		
		public:
			
			// ctor
			CMDRelationGPDB
				(
				IMemoryPool *pmp,
				IMDId *pmdid,
				CMDName *pmdname,
				BOOL fTemporary,
				Erelstoragetype erelstorage, 
				Ereldistrpolicy ereldistrpolicy,
				DrgPmdcol *pdrgpmdcol,
				DrgPul *pdrgpulDistrColumns,
				DrgPul *pdrgpulPartColumns,
				ULONG ulPartitions,
				BOOL fConvertHashToRandom,
				DrgPdrgPul *pdrgpdrgpul,
				DrgPmdid *pdrgpmdidIndices,
				DrgPmdid *pdrgpmdidTriggers,
				DrgPmdid *pdrgpmdidCheckConstraint,
				IMDPartConstraint *pmdpartcnstr,
				BOOL fHasOids
				);
			
			// dtor
			virtual
			~CMDRelationGPDB();
			
			// accessors
			virtual 
			const CWStringDynamic *Pstr() const
			{
				return m_pstr;
			}	
			
			// the metadata id
			virtual 
			IMDId *Pmdid() const;
			
			// relation name
			virtual 
			CMDName Mdname() const;
			
			// is this a temp relation
			virtual 
			BOOL FTemporary() const;
			
			// storage type (heap, appendonly, ...)
			virtual 
			Erelstoragetype Erelstorage() const; 
			
			// distribution policy (none, hash, random)
			virtual 
			Ereldistrpolicy Ereldistribution() const; 
			
			// number of columns
			virtual 
			ULONG UlColumns() const;
			
			// does relation have dropped columns
			virtual
			BOOL FHasDroppedColumns() const; 
			
			// number of non-dropped columns
			virtual 
			ULONG UlNonDroppedCols() const; 
			
			// return the absolute position of the given attribute position excluding dropped columns
			virtual 
			ULONG UlPosNonDropped(ULONG ulPos) const;
			
			// return the position of a column in the metadata object given the attribute number in the system catalog
			virtual
			ULONG UlPosFromAttno(INT iAttno) const;

			// return the original positions of all the non-dropped columns
			virtual
			DrgPul *PdrgpulNonDroppedCols() const;

			// retrieve the column at the given position
			virtual 
			const IMDColumn *Pmdcol(ULONG ulPos) const;
			
			// number of key sets
			virtual
			ULONG UlKeySets() const;
			
			// key set at given position
			virtual
			const DrgPul *PdrgpulKeyset(ULONG ulPos) const;
			
			// number of distribution columns
			virtual 
			ULONG UlDistrColumns() const;
			
			// retrieve the column at the given position in the distribution columns list for the relation
			virtual 
			const IMDColumn *PmdcolDistrColumn(ULONG ulPos) const;
			
			// return true if a hash distributed table needs to be considered as random
			virtual 
			BOOL FConvertHashToRandom() const;
			
			// does this table have oids
			virtual
			BOOL FHasOids() const;

			// is this a partitioned table
			virtual
			BOOL FPartitioned() const;
			
			// number of partition keys
			virtual
			ULONG UlPartColumns() const;
			
			// number of partitions
			virtual
			ULONG UlPartitions() const;

			// retrieve the partition key column at the given position
			virtual 
			const IMDColumn *PmdcolPartColumn(ULONG ulPos) const;

			// number of indices
			virtual 
			ULONG UlIndices() const;
			
			// number of triggers
			virtual
			ULONG UlTriggers() const;

			// retrieve the id of the metadata cache index at the given position
			virtual 
			IMDId *PmdidIndex(ULONG ulPos) const;
				
			// retrieve the id of the metadata cache trigger at the given position
			virtual
			IMDId *PmdidTrigger(ULONG ulPos) const;

			// serialize metadata relation in DXL format given a serializer object
			virtual 
			void Serialize(gpdxl::CXMLSerializer *) const;

			// number of check constraints
			virtual
			ULONG UlCheckConstraints() const;

			// retrieve the id of the check constraint cache at the given position
			virtual
			IMDId *PmdidCheckConstraint(ULONG ulPos) const;

			// part constraint
			virtual
			IMDPartConstraint *Pmdpartcnstr() const;

#ifdef GPOS_DEBUG
			// debug print of the metadata relation
			virtual 
			void DebugPrint(IOstream &os) const;
#endif
	};
}



#endif // !GPMD_CMDRelationGPDB_H

// EOF
