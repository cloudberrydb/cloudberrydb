//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarOneTimeFilter.h
//
//	@doc:
//		Class for representing a scalar filter that is executed once inside DXL physical operators.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarOneTimeFilter_H
#define GPDXL_CDXLScalarOneTimeFilter_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalarFilter.h"


namespace gpdxl
{
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarOneTimeFilter
	//
	//	@doc:
	//		Class for representing DXL filter operators
	//
	//---------------------------------------------------------------------------
	class CDXLScalarOneTimeFilter : public CDXLScalarFilter
	{
		private:
			// private copy ctor
		CDXLScalarOneTimeFilter(CDXLScalarOneTimeFilter&);
			
		public:
			// ctor
			explicit
			CDXLScalarOneTimeFilter(IMemoryPool *pmp);
			
			// accessors
			Edxlopid Edxlop() const;
			const CWStringConst *PstrOpName() const;
			
			// conversion function
			static
			CDXLScalarOneTimeFilter *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarOneTimeFilter == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarOneTimeFilter*>(pdxlop);
			}

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *, const CDXLNode *) const;

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
	};
}
#endif // !GPDXL_CDXLScalarOneTimeFilter_H

// EOF

