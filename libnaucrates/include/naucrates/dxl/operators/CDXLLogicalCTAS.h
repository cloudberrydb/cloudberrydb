//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CDXLLogicalCTAS.h
//
//	@doc:
//		Class for representing logical "CREATE TABLE AS" (CTAS) operator
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLLogicalCTAS_H
#define GPDXL_CDXLLogicalCTAS_H

#include "gpos/base.h"
#include "naucrates/md/IMDRelation.h"
#include "naucrates/dxl/operators/CDXLColDescr.h"

#include "naucrates/dxl/operators/CDXLLogical.h"

namespace gpdxl
{

	// fwd decl
	class CDXLCtasStorageOptions;
	
	using namespace gpmd;

	
	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLLogicalCTAS
	//
	//	@doc:
	//		Class for representing logical "CREATE TABLE AS" (CTAS) operator
	//
	//---------------------------------------------------------------------------
	class CDXLLogicalCTAS : public CDXLLogical
	{
		private:
			
			// mdid of table to create
			IMDId *m_pmdid;
			
			// schema name
			CMDName *m_pmdnameSchema;
			
			// table name
			CMDName *m_pmdnameRel;
			
			// list of columns
			DrgPdxlcd *m_pdrgpdxlcd;
			
			// storage options
			CDXLCtasStorageOptions *m_pdxlctasopt;
			
			// distribution policy
			IMDRelation::Ereldistrpolicy m_ereldistrpolicy;

			// list of distribution column positions		
			DrgPul *m_pdrgpulDistr;
			
			// is this a temporary table
			BOOL m_fTemporary;
			
			// does table have oids
			BOOL m_fHasOids;
			
			// storage type
			IMDRelation::Erelstoragetype m_erelstorage;
			
			// list of source column ids		
			DrgPul *m_pdrgpulSource;
			
			// list of vartypmod for target expressions
			// typemod records type-specific, e.g. the maximum length of a character column
			DrgPi *m_pdrgpiVarTypeMod;

			// private copy ctor
			CDXLLogicalCTAS(const CDXLLogicalCTAS &);
			
		public:
			
			// ctor
			CDXLLogicalCTAS
				(
				IMemoryPool *pmp, 
				IMDId *pmdid,
				CMDName *pmdnameSchema, 
				CMDName *pmdnameRel, 
				DrgPdxlcd *pdrgpdxcd,
				CDXLCtasStorageOptions *pdxlctasopt,
				IMDRelation::Ereldistrpolicy ereldistrpolicy,
				DrgPul *pdrgpulDistr, 
				BOOL fTemporary, 
				BOOL fHasOids, 
				IMDRelation::Erelstoragetype erelstorage,
				DrgPul *pdrgpulSource,
				DrgPi *pdrgpiVarTypeMod
				);
				
			// dtor
			virtual
			~CDXLLogicalCTAS();
		
			// operator type
			Edxlopid Edxlop() const;

			// operator name
			const CWStringConst *PstrOpName() const;

			// mdid of table to create
			IMDId *Pmdid() const
			{
				return m_pmdid;
			}
			
			// schema name
			CMDName *PmdnameSchema() const
			{
				return m_pmdnameSchema;
			}
			
			// table name
			CMDName *Pmdname() const
			{
				return m_pmdnameRel;
			}
			
			// column descriptors
			DrgPdxlcd *Pdrgpdxlcd() const
			{
				return m_pdrgpdxlcd;
			}
			
			// storage type
			IMDRelation::Erelstoragetype Erelstorage() const
			{
				return m_erelstorage;
			}
			
			// distribution policy
			IMDRelation::Ereldistrpolicy Ereldistrpolicy() const
			{
				return m_ereldistrpolicy;
			}
			
			// distribution column positions
			DrgPul *PdrgpulDistr() const
			{
				return m_pdrgpulDistr;
			}
		
			// source column ids
			DrgPul *PdrgpulSource() const
			{
				return m_pdrgpulSource;
			}
			
			// list of vartypmod for target expressions
			DrgPi *PdrgpiVarTypeMod() const
			{
				return m_pdrgpiVarTypeMod;
			}

			// is it a temporary table
			BOOL FTemporary() const
			{
				return m_fTemporary;
			}
			
			// does the table have oids
			BOOL FHasOids() const
			{
				return m_fHasOids;
			}
			
			// CTAS storage options
			CDXLCtasStorageOptions *Pdxlctasopt() const
			{
				return m_pdxlctasopt;
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *pdxln, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG

			// check if given column is defined by operator
			virtual
			BOOL FDefinesColumn(ULONG ulColId) const;

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLLogicalCTAS *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopLogicalCTAS == pdxlop->Edxlop());

				return dynamic_cast<CDXLLogicalCTAS*>(pdxlop);
			}

	};
}

#endif // !GPDXL_CDXLLogicalCTAS_H

// EOF

