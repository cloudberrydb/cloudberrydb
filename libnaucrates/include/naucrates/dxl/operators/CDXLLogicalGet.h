//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLLogicalGet.h
//
//	@doc:
//		Class for representing DXL logical get operators
//		
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLLogicalGet_H
#define GPDXL_CDXLLogicalGet_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLLogical.h"
#include "naucrates/dxl/operators/CDXLTableDescr.h"


namespace gpdxl
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLLogicalGet
	//
	//	@doc:
	//		Class for representing DXL logical get operators
	//
	//---------------------------------------------------------------------------
	class CDXLLogicalGet : public CDXLLogical
	{
		private:

			// table descriptor for the scanned table
			CDXLTableDescr *m_pdxltabdesc;

			// private copy ctor
			CDXLLogicalGet(CDXLLogicalGet&);

		public:
			// ctor
			CDXLLogicalGet(IMemoryPool *pmp, CDXLTableDescr *pdxltabdesc);

			// dtor
			virtual
			~CDXLLogicalGet();

			// accessors
			Edxlopid Edxlop() const;
			const CWStringConst *PstrOpName() const;
			CDXLTableDescr *Pdxltabdesc() const;

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// check if given column is defined by operator
			virtual
			BOOL FDefinesColumn(ULONG ulColId) const;

			// conversion function
			static
			CDXLLogicalGet *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopLogicalGet == pdxlop->Edxlop() ||
							EdxlopLogicalExternalGet == pdxlop->Edxlop());

				return dynamic_cast<CDXLLogicalGet*>(pdxlop);
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG

	};
}
#endif // !GPDXL_CDXLLogicalGet_H

// EOF
