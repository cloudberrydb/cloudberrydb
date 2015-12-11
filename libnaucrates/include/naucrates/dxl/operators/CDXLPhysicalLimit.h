//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLPhysicalLimit.h
//
//	@doc:
//		Class for representing DXL physical LIMIT operator
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLPhysicalLimit_H
#define GPDXL_CDXLPhysicalLimit_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLPhysical.h"

namespace gpdxl
{

	// indices of limit elements in the children array
	enum EdxlLimit
	{
		EdxllimitIndexProjList = 0,
		EdxllimitIndexChildPlan,
		EdxllimitIndexLimitCount,
		EdxllimitIndexLimitOffset
	};

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLPhysicalLimit
	//
	//	@doc:
	//		Class for representing DXL physical LIMIT operator
	//
	//---------------------------------------------------------------------------
	class CDXLPhysicalLimit : public CDXLPhysical
	{
		private:
			// private copy ctor
			CDXLPhysicalLimit(CDXLPhysicalLimit&);

		public:

			// ctor/dtor
			explicit
			CDXLPhysicalLimit(IMemoryPool *pmp);

			Edxlopid Edxlop() const;

			const CWStringConst *PstrOpName() const;

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLPhysicalLimit *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopPhysicalLimit == pdxlop->Edxlop());

				return dynamic_cast<CDXLPhysicalLimit*>(pdxlop);
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG
			
	};
}

#endif // !GPDXL_CDXLPhysicalLimit_H

//EOF
