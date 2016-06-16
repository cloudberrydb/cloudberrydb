//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLLogicalInsert.h
//
//	@doc:
//		Class for representing logical insert operators
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLLogicalInsert_H
#define GPDXL_CDXLLogicalInsert_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLLogical.h"

namespace gpdxl
{
	using namespace gpmd;

	// fwd decl
	class CDXLTableDescr;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLLogicalInsert
	//
	//	@doc:
	//		Class for representing logical insert operators
	//
	//---------------------------------------------------------------------------
	class CDXLLogicalInsert : public CDXLLogical
	{
		private:

			// target table descriptor
			CDXLTableDescr *m_pdxltabdesc;

			// list of source column ids		
			DrgPul *m_pdrgpul;
			
			// private copy ctor
			CDXLLogicalInsert(const CDXLLogicalInsert &);
			
		public:
			
			// ctor/dtor
			CDXLLogicalInsert(IMemoryPool *pmp, CDXLTableDescr *pdxltabdesc, DrgPul *pdrgpul);
						
			virtual
			~CDXLLogicalInsert();
		
			// operator type
			Edxlopid Edxlop() const;

			// operator name
			const CWStringConst *PstrOpName() const;

			// target table descriptor 
			CDXLTableDescr *Pdxltabdesc() const
			{
				return m_pdxltabdesc;
			}
			
			// source column ids
			DrgPul *Pdrgpul() const
			{
				return m_pdrgpul;
			}
			
#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *pdxln, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLLogicalInsert *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopLogicalInsert == pdxlop->Edxlop());

				return dynamic_cast<CDXLLogicalInsert*>(pdxlop);
			}

	};
}

#endif // !GPDXL_CDXLLogicalInsert_H

// EOF

