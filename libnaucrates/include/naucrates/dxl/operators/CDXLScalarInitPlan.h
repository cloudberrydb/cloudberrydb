//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarInitPlan.h
//
//	@doc:
//		Class for representing DXL Scalar InitPlan operator
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarInitPlan_H
#define GPDXL_CDXLScalarInitPlan_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"

namespace gpdxl
{
	using namespace gpos;

	// indices of InitPlan elements in the children array
	enum EdxlInitPlan
	{
		EdxlInitPlanIndexChildPlan,
		EdxlInitPlanIndexSentinel
	};

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarInitPlan
	//
	//	@doc:
	//		Class for representing DXL physical InitPlan operator
	//
	//---------------------------------------------------------------------------
	class CDXLScalarInitPlan : public CDXLScalar
	{
		private:
			// private copy ctor
			CDXLScalarInitPlan(CDXLScalarInitPlan&);

		public:

			// ctor/dtor
			explicit
			CDXLScalarInitPlan(IMemoryPool *pmp);

			Edxlopid Edxlop() const;

			const CWStringConst *PstrOpName() const;

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *, const CDXLNode *) const;

			// conversion function
			static
			CDXLScalarInitPlan *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarInitPlan == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarInitPlan*>(pdxlop);
			}

			// does the operator return a boolean result
			virtual
			BOOL FBoolean
					(
					CMDAccessor *//pmda
					)
					const
			{
				return false;
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *pdxln, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG

	};
}

#endif // !GPDXL_CDXLScalarInitPlan_H

//EOF
