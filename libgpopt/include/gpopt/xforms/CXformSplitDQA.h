//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal Inc.
//
//	@filename:
//		CXformSplitDQA.h
//
//	@doc:
//		Split an aggregate into a three level aggregation
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformSplitDQA_H
#define GPOPT_CXformSplitDQA_H

#include "gpos/base.h"
#include "gpopt/xforms/CXformExploration.h"
#include "gpopt/base/CUtils.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CXformSplitDQA
	//
	//	@doc:
	//		Split an aggregate into a three level aggregation
	//
	//---------------------------------------------------------------------------
	class CXformSplitDQA : public CXformExploration
	{

		private:

			// hash map between expression and a column reference
			typedef CHashMap<CExpression, CColRef, CExpression::UlHash, CUtils::FEqual,
						CleanupRelease<CExpression>, CleanupNULL<CColRef> > HMExprCr;

			// private copy ctor
			CXformSplitDQA(const CXformSplitDQA &);

			// generate an expression with multi-level aggregation
			static
			CExpression *PexprMultiLevelAggregation
							(
							IMemoryPool *pmp,
							CExpression *pexprRelational,
							DrgPexpr *pdrgpexprPrElFirstStage,
							DrgPexpr *pdrgpexprPrElSecondStage,
							DrgPexpr *pdrgpexprPrElThirdStage,
							DrgPcr *pdrgpcrArgDQA,
							DrgPcr *pdrgpcrLastStage,
							BOOL fSplit2LevelsOnly,
							BOOL fAddDistinctColToLocalGb
							);

			// split DQA into a local DQA and global non-DQA aggregate function
			static
			CExpression *PexprSplitIntoLocalDQAGlobalAgg
							(
							IMemoryPool *pmp,
							CColumnFactory *pcf,
							CMDAccessor *pmda,
							CExpression *pexpr,
							CExpression *pexprRelational,
							HMExprCr *phmexprcr,
							DrgPcr *pdrgpcrArgDQA
							);

			// helper function to split DQA
			static
			CExpression *PexprSplitHelper
				(
				IMemoryPool *pmp,
				CColumnFactory *pcf,
				CMDAccessor *pmda,
				CExpression *pexpr,
				CExpression *pexprRelational,
				HMExprCr *phmexprcr,
				DrgPcr *pdrgpcrArgDQA,
				BOOL fScalarAggregate
				);

			// given a scalar aggregate generate the local, intermediate and global
			// aggregate functions. Then add them to the project list of the 
			// corresponding aggregate operator at each stage of the multi-stage
			// aggregation
			static
			void PopulatePrLMultiPhaseAgg
					(
					IMemoryPool *pmp,
					CColumnFactory *pcf,
					CMDAccessor *pmda,
					CExpression *pexprPrEl,
					DrgPexpr *pdrgpexprPrElFirstStage,
					DrgPexpr *pdrgpexprPrElSecondStage,
					DrgPexpr *pdrgpexprPrElLastStage,
					BOOL fSplit2LevelsOnly
					);

			// create project element for the aggregate function of a particular level
			static
			CExpression *PexprPrElAgg
							(
							IMemoryPool *pmp,
							CExpression *pexprAggFunc,
							EAggfuncStage eaggfuncstage,
							CColRef *pcrPreviousStage,
							CColRef *pcrGlobal
							);

			// extract arguments of distinct aggs
			static
			void ExtractDistinctCols
					(
					IMemoryPool *pmp,
					CColumnFactory *pcf,
					CMDAccessor *pmda,
					CExpression *pexpr,
					DrgPexpr *pdrgpexprChildPrEl,
					HMExprCr *phmexprcr,
					DrgPcr **ppdrgpcrArgDQA
					);

			// return the column reference of the argument to the aggregate function
			static
			CColRef *PcrAggFuncArgument
						(
						IMemoryPool *pmp,
						CMDAccessor *pmda,
						CColumnFactory *pcf,
						CExpression *pexprArg,
						DrgPexpr *pdrgpexprChildPrEl
						);

		public:

			// ctor
			explicit
			CXformSplitDQA(IMemoryPool *pmp);

			// dtor
			virtual
			~CXformSplitDQA()
			{}

			// ident accessors
			virtual
			EXformId Exfid() const
			{
				return ExfSplitDQA;
			}

			// return a string for xform name
			virtual
			const CHAR *SzId() const
			{
				return "CXformSplitDQA";
			}

			// Compatibility function for splitting aggregates
			virtual
			BOOL FCompatible(CXform::EXformId exfid)
			{
				return (CXform::ExfSplitDQA != exfid) &&
						(CXform::ExfSplitGbAgg != exfid);
			}

			// compute xform promise for a given expression handle
			virtual
			EXformPromise Exfp (CExpressionHandle &exprhdl) const;

			// actual transform
			void Transform(CXformContext *, CXformResult *, CExpression *) const;

	}; // class CXformSplitDQA

}

#endif // !GPOPT_CXformSplitDQA_H

// EOF
