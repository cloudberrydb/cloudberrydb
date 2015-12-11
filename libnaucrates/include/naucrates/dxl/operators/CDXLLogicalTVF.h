//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLLogicalTVF.h
//
//	@doc:
//		Class for representing table-valued functions
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLLogicalTVF_H
#define GPDXL_CDXLLogicalTVF_H

#include "gpos/base.h"
#include "naucrates/md/IMDId.h"
#include "naucrates/dxl/operators/CDXLLogical.h"
#include "naucrates/dxl/operators/CDXLColDescr.h"

namespace gpdxl
{
	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLLogicalTVF
	//
	//	@doc:
	//		Class for representing table-valued functions
	//
	//---------------------------------------------------------------------------
	class CDXLLogicalTVF : public CDXLLogical
	{
		private:
			// catalog id of the function
			IMDId *m_pmdidFunc;

			// return type
			IMDId *m_pmdidRetType;

			// function name
			CMDName *m_pmdname;

			// list of column descriptors		
			DrgPdxlcd *m_pdrgdxlcd;
			
			// private copy ctor
			CDXLLogicalTVF(const CDXLLogicalTVF &);
			
		public:
			// ctor/dtor
			CDXLLogicalTVF
				(
				IMemoryPool *pmp,
				IMDId *pmdidFunc,
				IMDId *pmdidRetType,
				CMDName *pmdname,
				DrgPdxlcd *pdrgdxlcd
				);
						
			virtual
			~CDXLLogicalTVF();
		
			// get operator type
			Edxlopid Edxlop() const;

			// get operator name
			const CWStringConst *PstrOpName() const;

			// get function name
			CMDName *Pmdname() const
			{
				return m_pmdname;
			}

			// get function id
			IMDId *PmdidFunc() const
			{
				return m_pmdidFunc;
			}

			// get return type
			IMDId *PmdidRetType() const
			{
				return m_pmdidRetType;
			}

			// get number of output columns
			ULONG UlArity() const;
			
			// return the array of column descriptors
			const DrgPdxlcd *Pdrgpdxlcd() const
			{
				return m_pdrgdxlcd;
			}

			// get the column descriptor at the given position
			const CDXLColDescr *Pdxlcd(ULONG ul) const;

			// check if given column is defined by operator
			virtual
			BOOL FDefinesColumn(ULONG ulColId) const;

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLLogicalTVF *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopLogicalTVF == pdxlop->Edxlop());

				return dynamic_cast<CDXLLogicalTVF*>(pdxlop);
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG

	};
}

#endif // !GPDXL_CDXLLogicalTVF_H

// EOF

