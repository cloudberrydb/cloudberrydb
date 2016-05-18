//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CMDRelationExternalGPDB.h
//
//	@doc:
//		Class representing MD external relations
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPMD_CMDRelationExternalGPDB_H
#define GPMD_CMDRelationExternalGPDB_H

#include "gpos/base.h"
#include "gpos/string/CWStringDynamic.h"

#include "naucrates/md/IMDRelationExternal.h"
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
	//		CMDRelationExternalGPDB
	//
	//	@doc:
	//		Class representing MD external relations
	//
	//---------------------------------------------------------------------------
	class CMDRelationExternalGPDB : public IMDRelationExternal
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

			// array of key sets
			DrgPdrgPul *m_pdrgpdrgpulKeys;

			// array of index ids
			DrgPmdid *m_pdrgpmdidIndices;

			// array of trigger ids
			DrgPmdid *m_pdrgpmdidTriggers;

			// array of check constraint mdids
			DrgPmdid *m_pdrgpmdidCheckConstraint;

			// reject limit
			INT m_iRejectLimit;

			// reject limit specified as number of rows (as opposed to a percentage)?
			BOOL m_fRejLimitInRows;

			// format error table mdid
			IMDId *m_pmdidFmtErrRel;

			// mapping of column position to positions excluding dropped columns
			HMUlUl *m_phmululNonDroppedCols;
			
			// mapping of attribute number in the system catalog to the positions of
			// the non dropped column in the metadata object
			HMIUl *m_phmiulAttno2Pos;

			// the original positions of all the non-dropped columns
			DrgPul *m_pdrgpulNonDroppedCols;

			// format type for the relation
			const CWStringConst *PstrFormatType() const;

			// private copy ctor
			CMDRelationExternalGPDB(const CMDRelationExternalGPDB &);

		public:

			// ctor
			CMDRelationExternalGPDB
				(
				IMemoryPool *pmp,
				IMDId *pmdid,
				CMDName *pmdname,
				Ereldistrpolicy ereldistrpolicy,
				DrgPmdcol *pdrgpmdcol,
				DrgPul *pdrgpulDistrColumns,
				BOOL fConvertHashToRandom,
				DrgPdrgPul *pdrgpdrgpul,
				DrgPmdid *pdrgpmdidIndices,
				DrgPmdid *pdrgpmdidTriggers,
				DrgPmdid *pdrgpmdidCheckConstraint,
				INT iRejectLimit,
				BOOL fRejLimitInRows,
				IMDId *pmdidFmtErrRel
				);

			// dtor
			virtual
			~CMDRelationExternalGPDB();

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
			
			// return the original positions of all the non-dropped columns
			virtual
			DrgPul *PdrgpulNonDroppedCols() const;

			// return true if a hash distributed table needs to be considered as random
			virtual
			BOOL FConvertHashToRandom() const;

			// reject limit
			virtual
			INT IRejectLimit() const;

			// reject limit in rows?
			virtual
			BOOL FRejLimitInRows() const;

			// format error table mdid
			virtual
			IMDId *PmdidFmtErrRel() const;

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

			// number of indices
			virtual
			ULONG UlIndices() const;

			// number of triggers
			virtual
			ULONG UlTriggers() const;

			// return the absolute position of the given attribute position excluding dropped columns
			virtual 
			ULONG UlPosNonDropped(ULONG ulPos) const;

			 // return the position of a column in the metadata object given the attribute number in the system catalog
			virtual
			ULONG UlPosFromAttno(INT iAttno) const;

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

#ifdef GPOS_DEBUG
			// debug print of the metadata relation
			virtual
			void DebugPrint(IOstream &os) const;
#endif
	};
}

#endif // !GPMD_CMDRelationExternalGPDB_H

// EOF
