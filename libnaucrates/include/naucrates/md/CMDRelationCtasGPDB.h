//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CMDRelationCtasGPDB.h
//
//	@doc:
//		Class representing MD CTAS relations
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPMD_CMDRelationCTASGPDB_H
#define GPMD_CMDRelationCTASGPDB_H

#include "gpos/base.h"
#include "gpos/string/CWStringDynamic.h"

#include "naucrates/md/IMDRelationCtas.h"
#include "naucrates/md/IMDColumn.h"
#include "naucrates/md/CMDColumn.h"
#include "naucrates/md/CMDName.h"

namespace gpdxl
{
	// fwd decl
	class CXMLSerializer;
	class CDXLCtasStorageOptions;
}

namespace gpmd
{
	using namespace gpos;
	using namespace gpdxl;


	//---------------------------------------------------------------------------
	//	@class:
	//		CMDRelationCtasGPDB
	//
	//	@doc:
	//		Class representing MD CTAS relations
	//
	//---------------------------------------------------------------------------
	class CMDRelationCtasGPDB : public IMDRelationCtas
	{
		private:
			// memory pool
			IMemoryPool *m_pmp;

			// DXL for object
			const CWStringDynamic *m_pstr;

			// relation mdid
			IMDId *m_pmdid;

			// schema name
			CMDName *m_pmdnameSchema;
			
			// table name
			CMDName *m_pmdname;
			
			// is this a temporary relation
			BOOL m_fTemporary;
			
			// does this table have oids
			BOOL m_fHasOids;
			
			// storage type
			Erelstoragetype m_erelstorage;

			// distribution policy
			Ereldistrpolicy m_ereldistrpolicy;

			// columns
			DrgPmdcol *m_pdrgpmdcol;

			// indices of distribution columns
			DrgPul *m_pdrgpulDistrColumns;
			
			// array of key sets
			DrgPdrgPul *m_pdrgpdrgpulKeys;
			
			// mapping of attribute number in the system catalog to the positions of
			// the non dropped column in the metadata object
			HMIUl *m_phmiulAttno2Pos;

			// the original positions of all the non-dropped columns
			DrgPul *m_pdrgpulNonDroppedCols;

			// storage options
			CDXLCtasStorageOptions *m_pdxlctasopt;

			// vartypemod list
			DrgPi *m_pdrgpiVarTypeMod;

			// private copy ctor
			CMDRelationCtasGPDB(const CMDRelationCtasGPDB &);

		public:

			// ctor
			CMDRelationCtasGPDB
				(
				IMemoryPool *pmp,
				IMDId *pmdid,
				CMDName *pmdnameSchema,
				CMDName *pmdname,
				BOOL fTemporary,
				BOOL fHasOids,
				Erelstoragetype erelstorage,
				Ereldistrpolicy ereldistrpolicy,
				DrgPmdcol *pdrgpmdcol,
				DrgPul *pdrgpulDistrColumns,
				DrgPdrgPul *pdrgpdrgpulKeys,
				CDXLCtasStorageOptions *pdxlctasopt,
				DrgPi *pdrgpiVarTypeMod
				);

			// dtor
			virtual
			~CMDRelationCtasGPDB();

			// accessors
			virtual
			const CWStringDynamic *Pstr() const
			{
				return m_pstr;
			}

			// the metadata id
			virtual
			IMDId *Pmdid() const;

			// schema name
			virtual
			CMDName *PmdnameSchema() const;
			
			// relation name
			virtual
			CMDName Mdname() const;

			// distribution policy (none, hash, random)
			virtual
			Ereldistrpolicy Ereldistribution() const;

			// does this table have oids
			virtual
			BOOL FHasOids() const
			{
				return m_fHasOids;
			}
			
			// is this a temp relation
			virtual 
			BOOL FTemporary() const
			{
				return m_fTemporary;
			}
			
			// storage type
			virtual 
			Erelstoragetype Erelstorage() const
			{
				return m_erelstorage;
			}
			
			// CTAS storage options
			virtual 
			CDXLCtasStorageOptions *Pdxlctasopt() const
			{
				return m_pdxlctasopt;
			}
			
			// number of columns
			virtual
			ULONG UlColumns() const;
			
			// does relation have dropped columns
			virtual
			BOOL FHasDroppedColumns() const
			{
				return false;
			}

			// number of non-dropped columns
			virtual 
			ULONG UlNonDroppedCols() const
			{
				return UlColumns();
			}
			
			// return the original positions of all the non-dropped columns
			virtual
			DrgPul *PdrgpulNonDroppedCols() const
			{
				return m_pdrgpulNonDroppedCols;
			}

			// retrieve the column at the given position
			virtual
			const IMDColumn *Pmdcol(ULONG ulPos) const;

			// number of distribution columns
			virtual
			ULONG UlDistrColumns() const;

			// retrieve the column at the given position in the distribution columns list for the relation
			virtual
			const IMDColumn *PmdcolDistrColumn(ULONG ulPos) const;

			// number of indices
			virtual
			ULONG UlIndices() const
			{
				return 0;
			}

			// number of triggers
			virtual
			ULONG UlTriggers() const
			{
				return 0;
			}
			
			// return the absolute position of the given attribute position excluding dropped columns
			virtual 
			ULONG UlPosNonDropped(ULONG ulPos) const
			{
				return ulPos;
			}
			
			 // return the position of a column in the metadata object given the attribute number in the system catalog
			virtual
			ULONG UlPosFromAttno(INT iAttno) const;

			// retrieve the id of the metadata cache index at the given position
			virtual
			IMDId *PmdidIndex
				(
				ULONG // ulPos
				) 
				const
			{
				GPOS_ASSERT("CTAS tables have no indexes"); 
				return 0;
			}

			// retrieve the id of the metadata cache trigger at the given position
			virtual
			IMDId *PmdidTrigger
				(
				ULONG // ulPos
				) 
				const
			{
				GPOS_ASSERT("CTAS tables have no triggers"); 
				return 0;
			}

			// serialize metadata relation in DXL format given a serializer object
			virtual
			void Serialize(gpdxl::CXMLSerializer *) const;

			// number of check constraints
			virtual
			ULONG UlCheckConstraints() const
			{
				return 0;
			}

			// retrieve the id of the check constraint cache at the given position
			virtual
			IMDId *PmdidCheckConstraint
				(
				ULONG // ulPos
				) 
				const
			{
				GPOS_ASSERT("CTAS tables have no constraints"); 
				return 0;
			}

			// list of vartypmod for target expressions
			DrgPi *PdrgpiVarTypeMod() const
			{
				return m_pdrgpiVarTypeMod;
			}

#ifdef GPOS_DEBUG
			// debug print of the metadata relation
			virtual
			void DebugPrint(IOstream &os) const;
#endif
	};
}

#endif // !GPMD_CMDRelationCTASGPDB_H

// EOF
