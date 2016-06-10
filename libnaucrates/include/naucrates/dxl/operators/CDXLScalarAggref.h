//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarAggref.h
//
//	@doc:
//		Class for representing DXL AggRef
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarAggref_H
#define GPDXL_CDXLScalarAggref_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"
#include "naucrates/md/IMDId.h"

namespace gpdxl
{
	using namespace gpmd;

	enum EdxlAggrefStage
	{
		EdxlaggstageNormal = 0,
		EdxlaggstagePartial, // First (lower, earlier) stage of 2-stage aggregation.
		EdxlaggstageIntermediate, // Between AGGSTAGE_PARTIAL and AGGSTAGE_FINAL that handles the higher aggregation
								  // level in a (partial) ROLLUP grouping extension query
		EdxlaggstageFinal, // Second (upper, later) stage of 2-stage aggregation.
		EdxlaggstageSentinel
	};

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarAggref
	//
	//	@doc:
	//		Class for representing DXL AggRef
	//
	//---------------------------------------------------------------------------
	class CDXLScalarAggref : public CDXLScalar
	{
		private:

			// catalog id of the function
			IMDId *m_pmdidAgg;

			// resolved return type refers to a non-ambiguous type that was resolved during query
			// parsing if the actual return type of Agg is ambiguous (e.g., AnyElement in GPDB)
			// if resolved return type is NULL, then we can get Agg return type by looking up MD cache
			// using Agg MDId
			IMDId *m_pmdidResolvedRetType;

			// Denotes whether it's agg(DISTINCT ...)
			BOOL m_fDistinct;

			// Denotes the MPP Stage
			EdxlAggrefStage m_edxlaggstage;

			// private copy ctor
			CDXLScalarAggref(const CDXLScalarAggref&);

		public:
			// ctor/dtor
			CDXLScalarAggref
				(
				IMemoryPool *pmp,
				IMDId *pmdidAgg,
				IMDId *pmdidResolvedRetType,
				BOOL fDistinct,
				EdxlAggrefStage edxlaggstage
				);

			virtual
			~CDXLScalarAggref();

			// ident accessors
			Edxlopid Edxlop() const;

			const CWStringConst *PstrOpName() const;

			IMDId *PmdidAgg() const;

			IMDId *PmdidResolvedRetType() const;

			const CWStringConst *PstrAggStage() const;

			EdxlAggrefStage Edxlaggstage() const;

			BOOL FDistinct() const;

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLScalarAggref *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarAggref == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarAggref*>(pdxlop);
			}

			// does the operator return a boolean result
			virtual
			BOOL FBoolean(CMDAccessor *pmda) const;

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *pdxln, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG
	};
}

#endif // !GPDXL_CDXLScalarAggref_H

// EOF
