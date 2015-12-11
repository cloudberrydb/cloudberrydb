//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CDXLLogicalLimit.h
//
//	@doc:
//		Class for representing DXL logical limit operators
//		
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPDXL_CDXLLogicalLimit_H
#define GPDXL_CDXLLogicalLimit_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/operators/CDXLLogical.h"

namespace gpdxl
{
	// indices of limit elements in the children array
	enum EdxlLogicalLimit
	{
		EdxllogicallimitIndexSortColList =0,
		EdxllogicallimitIndexLimitCount,
		EdxllogicallimitIndexLimitOffset,
		EdxllogicallimitIndexChildPlan
	};


	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLLogicalLimit
	//
	//	@doc:
	//		Class for representing DXL Logical limit operators
	//
	//---------------------------------------------------------------------------
	class CDXLLogicalLimit : public CDXLLogical
	{
		private:
			BOOL m_fTopLimitUnderDML;

			// private copy ctor
			CDXLLogicalLimit(CDXLLogicalLimit&);

		public:
			// ctor/dtor
			CDXLLogicalLimit(IMemoryPool *pmp, BOOL fNonRemovableLimit);

			virtual
			~CDXLLogicalLimit();

			// accessors
			Edxlopid Edxlop() const;

			const CWStringConst *PstrOpName() const;

			// the limit is right under a DML or CTAS
			BOOL FTopLimitUnderDML() const
			{
				return m_fTopLimitUnderDML;
			}

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLLogicalLimit *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopLogicalLimit == pdxlop->Edxlop());

				return dynamic_cast<CDXLLogicalLimit*>(pdxlop);
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG

	};
}
#endif // !GPDXL_CDXLLogicalLimit_H

// EOF
