//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLPhysicalSequence.h
//
//	@doc:
//		Class for representing DXL physical sequence operators
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLPhysicalSequence_H
#define GPDXL_CDXLPhysicalSequence_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLPhysical.h"
#include "naucrates/dxl/operators/CDXLSpoolInfo.h"


namespace gpdxl
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLPhysicalSequence
	//
	//	@doc:
	//		Class for representing DXL physical sequence operators
	//
	//---------------------------------------------------------------------------
	class CDXLPhysicalSequence : public CDXLPhysical
	{
		private:

			// private copy ctor
			CDXLPhysicalSequence(CDXLPhysicalSequence&);

		public:
			// ctor
			CDXLPhysicalSequence(IMemoryPool *pmp);
			
			// dtor
			virtual
			~CDXLPhysicalSequence();
			
			// accessors
			Edxlopid Edxlop() const;
			const CWStringConst *PstrOpName() const;

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLPhysicalSequence *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopPhysicalSequence == pdxlop->Edxlop());

				return dynamic_cast<CDXLPhysicalSequence*>(pdxlop);
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG

	};
}
#endif // !GPDXL_CDXLPhysicalSequence_H

// EOF

