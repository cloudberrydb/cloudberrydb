//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarFilter.h
//
//	@doc:
//		Class for representing a scalar filter node inside DXL physical operators.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarFilter_H
#define GPDXL_CDXLScalarFilter_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"


namespace gpdxl
{
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarFilter
	//
	//	@doc:
	//		Class for representing DXL filter operators
	//
	//---------------------------------------------------------------------------
	class CDXLScalarFilter : public CDXLScalar
	{
		private:
			// private copy ctor
			CDXLScalarFilter(CDXLScalarFilter&);
			
		public:
			// ctor/dtor
			explicit
			CDXLScalarFilter(IMemoryPool *pmp);
			
			// accessors
			Edxlopid Edxlop() const;
			const CWStringConst *PstrOpName() const;
			
			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLScalarFilter *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarFilter == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarFilter*>(pdxlop);
			}

			// does the operator return a boolean result
			virtual
			BOOL FBoolean
					(
					CMDAccessor *//pmda
					)
					const
			{
				GPOS_ASSERT(!"Invalid function call for a container operator");
				return false;
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure
			virtual
			void AssertValid(const CDXLNode *pdxln, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG	
	};
}
#endif // !GPDXL_CDXLScalarFilter_H

// EOF

