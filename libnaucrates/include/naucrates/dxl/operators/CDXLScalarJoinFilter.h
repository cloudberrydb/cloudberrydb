//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarJoinFilter.h
//
//	@doc:
//		Class for representing a join filter node inside DXL join operators.
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarJoinFilter_H
#define GPDXL_CDXLScalarJoinFilter_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalarFilter.h"


namespace gpdxl
{
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarJoinFilter
	//
	//	@doc:
	//		Class for representing DXL join condition operators
	//
	//---------------------------------------------------------------------------
	class CDXLScalarJoinFilter : public CDXLScalarFilter
	{
		private:
			// private copy ctor
			CDXLScalarJoinFilter(CDXLScalarJoinFilter&);

		public:
			// ctor/dtor
			explicit
			CDXLScalarJoinFilter(IMemoryPool *pmp);
			
			// accessors
			Edxlopid Edxlop() const;
			const CWStringConst *PstrOpName() const;
			
			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *, const CDXLNode *) const;

			// conversion function
			static
			CDXLScalarJoinFilter *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarJoinFilter == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarJoinFilter*>(pdxlop);
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
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *pdxln, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG
			
	};
}
#endif // !GPDXL_CDXLScalarJoinFilter_H

// EOF

