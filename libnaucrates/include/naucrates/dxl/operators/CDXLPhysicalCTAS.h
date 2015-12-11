//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CDXLPhysicalCTAS.h
//
//	@doc:
//		Class for representing DXL physical CTAS operators
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPDXL_CDXLPhysicalCTAS_H
#define GPDXL_CDXLPhysicalCTAS_H

#include "gpos/base.h"
#include "naucrates/md/IMDRelation.h"
#include "naucrates/dxl/operators/CDXLColDescr.h"

#include "naucrates/dxl/operators/CDXLPhysical.h"

namespace gpdxl
{

	// fwd decl
	class CDXLCtasStorageOptions;
	
	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLPhysicalCTAS
	//
	//	@doc:
	//		Class for representing DXL physical CTAS operators
	//
	//---------------------------------------------------------------------------
	class CDXLPhysicalCTAS : public CDXLPhysical
	{
		private:

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

			// list of vartypmod
			DrgPi *m_pdrgpiVarTypeMod;

			// private copy ctor
			CDXLPhysicalCTAS(CDXLPhysicalCTAS&);

		public:
			// ctor
			CDXLPhysicalCTAS
				(
				IMemoryPool *pmp, 
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
			~CDXLPhysicalCTAS();

			// operator type
			Edxlopid Edxlop() const;

			// operator name
			const CWStringConst *PstrOpName() const;

			// column descriptors
			DrgPdxlcd *Pdrgpdxlcd() const
			{
				return m_pdrgpdxlcd;
			}
			
			// distribution type
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

			// table name
			CMDName *PmdnameSchema() const
			{
				return m_pmdnameSchema;
			}
			
			// table name
			CMDName *Pmdname() const
			{
				return m_pmdnameRel;
			}
			
			// is temporary
			BOOL FTemporary() const
			{
				return m_fTemporary;
			}

			// CTAS storage options
			CDXLCtasStorageOptions *Pdxlctasopt() const
			{
				return m_pdxlctasopt;
			}
			
			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *pdxln, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG

			// conversion function
			static
			CDXLPhysicalCTAS *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopPhysicalCTAS == pdxlop->Edxlop());
				return dynamic_cast<CDXLPhysicalCTAS*>(pdxlop);
			}
	};
}
#endif // !GPDXL_CDXLPhysicalCTAS_H

// EOF
