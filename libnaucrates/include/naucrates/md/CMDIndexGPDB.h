//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CMDIndexGPDB.h
//
//	@doc:
//		Implementation of indexes in the metadata cache
//---------------------------------------------------------------------------



#ifndef GPMD_CMDIndexGPDB_H
#define GPMD_CMDIndexGPDB_H

#include "gpos/base.h"
#include "naucrates/md/IMDIndex.h"

namespace gpmd
{
	using namespace gpos;
	using namespace gpdxl;
	
	// fwd decl
	class IMDPartConstraint;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CMDIndexGPDB
	//
	//	@doc:
	//		Class for indexes in the metadata cache
	//
	//---------------------------------------------------------------------------
	class CMDIndexGPDB : public IMDIndex
	{
		private:
			
			// memory pool
			IMemoryPool *m_pmp;
			
			// index mdid
			IMDId *m_pmdid;
						
			// table name
			CMDName *m_pmdname;

			// is the index clustered
			BOOL m_fClustered;

			// index type
			EmdindexType m_emdindt;
			
			// type of items returned by index
			IMDId *m_pmdidItemType;

			// index key columns
			DrgPul *m_pdrgpulKeyCols;

			// included columns 
			DrgPul *m_pdrgpulIncludedCols;

			// operator classes for each index key
			DrgPmdid *m_pdrgpmdidOpClasses;
			
			// partition constraint
			IMDPartConstraint *m_pmdpartcnstr;
			
			// DXL for object
			const CWStringDynamic *m_pstr;
		
			// private copy ctor
			CMDIndexGPDB(const CMDIndexGPDB &);
			
		public:
			
			// ctor
			CMDIndexGPDB
				(
				IMemoryPool *pmp, 
				IMDId *pmdid, 
				CMDName *pmdname,
				BOOL fClustered, 
				EmdindexType emdindt,
				IMDId *pmdidItemType,
				DrgPul *pdrgpulKeyCols,
				DrgPul *pdrgpulIncludedCols,
				DrgPmdid *pdrgpmdidOpClasses,
				IMDPartConstraint *pmdpartcnstr
				);
			
			// dtor
			virtual
			~CMDIndexGPDB();
			
			// index mdid
			virtual 
			IMDId *Pmdid() const;
			
			// index name
			virtual 
			CMDName Mdname() const;

			// is the index clustered
			virtual
			BOOL FClustered() const;
			
			// index type
			virtual
			EmdindexType Emdindt() const;
			
			// number of keys
			virtual
			ULONG UlKeys() const;
			
			// return the n-th key column
			virtual
			ULONG UlKey(ULONG ulPos) const;
			
			// return the position of the key column
			virtual
			ULONG UlPosInKey(ULONG ulCol) const;

			// number of included columns
			virtual
			ULONG UlIncludedCols() const;

			// return the n-th included column
			virtual
			ULONG UlIncludedCol(ULONG ulPos) const;

			// return the position of the included column
			virtual
			ULONG UlPosInIncludedCol(ULONG ulCol) const;

			// part constraint
			virtual
			IMDPartConstraint *Pmdpartcnstr() const;

			// DXL string for index
			virtual 
			const CWStringDynamic *Pstr() const
			{
				return m_pstr;
			}	
			
			// serialize MD index in DXL format given a serializer object
			virtual 
			void Serialize(gpdxl::CXMLSerializer *) const;

			// type id of items returned by the index
			virtual
			IMDId *PmdidItemType() const;
			
			// check if given scalar comparison can be used with the index key 
			// at the specified position
			virtual 
			BOOL FCompatible(const IMDScalarOp *pmdscop, ULONG ulKEyPos) const;

#ifdef GPOS_DEBUG
			// debug print of the MD index
			virtual 
			void DebugPrint(IOstream &os) const;
#endif
	};
}

#endif // !GPMD_CMDIndexGPDB_H

// EOF
