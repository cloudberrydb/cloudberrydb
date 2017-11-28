//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLScalarSubPlan.h
//
//	@doc:
//		Class for representing DXL Scalar SubPlan operator
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarSubPlan_H
#define GPDXL_CDXLScalarSubPlan_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"
#include "naucrates/dxl/operators/CDXLColRef.h"

namespace gpdxl
{
	using namespace gpos;

	// indices of SubPlan elements in the children array
	enum EdxlSubPlan
	{
		EdxlSubPlanIndexChildPlan,
		EdxlSubPlanIndexSentinel
	};

	// subplan type
	enum EdxlSubPlanType
	{
		EdxlSubPlanTypeScalar = 0,	// subplan for scalar subquery
		EdxlSubPlanTypeExists,		// subplan for exists subquery
		EdxlSubPlanTypeNotExists,	// subplan for not exists subquery
		EdxlSubPlanTypeAny,			// subplan for quantified (ANY/IN) subquery
		EdxlSubPlanTypeAll,			// subplan for quantified (ALL/NOT IN) subquery

		EdxlSubPlanTypeSentinel
	};

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarSubPlan
	//
	//	@doc:
	//		Class for representing DXL scalar SubPlan operator
	//
	//---------------------------------------------------------------------------
	class CDXLScalarSubPlan : public CDXLScalar
	{
		private:

			// catalog MDId of the first column type
			IMDId *m_pmdidFirstColType;

			// array of outer column references
			DrgPdxlcr *m_pdrgdxlcr;

			// subplan type
			EdxlSubPlanType m_edxlsubplantype;

			// test expression -- not null if quantified/existential subplan
			CDXLNode *m_pdxlnTestExpr;

			// private copy ctor
			CDXLScalarSubPlan(CDXLScalarSubPlan&);

		public:

			// ctor/dtor
			CDXLScalarSubPlan
				(
				IMemoryPool *pmp,
				IMDId *pmdidFirstColType,
				DrgPdxlcr *pdrgdxlcr,
				EdxlSubPlanType edxlsubplantype,
				CDXLNode *pdxlnTestExpr
				);

			virtual
			~CDXLScalarSubPlan();

			// Operator type
			Edxlopid Edxlop() const
			{
				return EdxlopScalarSubPlan;
			}

			// Operator name
			const CWStringConst *PstrOpName() const;

			// type of first output column
			IMDId *PmdidFirstColType() const;

			// outer references
			const DrgPdxlcr *DrgdxlcrOuterRefs() const
			{
				return m_pdrgdxlcr;
			}

			// return subplan type
			EdxlSubPlanType Edxlsptype() const
			{
				return m_edxlsubplantype;
			}

			// return test expression
			CDXLNode *PdxlnTestExpr() const
			{
				return m_pdxlnTestExpr;
			}

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLScalarSubPlan *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarSubPlan == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarSubPlan*>(pdxlop);
			}

			// does the operator return a boolean result
			virtual
			BOOL FBoolean(CMDAccessor *pmda) const;

			// return a string representation of Subplan type
			const CWStringConst *PstrSubplanType() const;

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *pdxln, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG

	};
}

#endif // !GPDXL_CDXLScalarSubPlan_H

//EOF
