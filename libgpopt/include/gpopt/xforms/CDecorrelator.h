//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CDecorrelator.h
//
//	@doc:
//		Decorrelation processor
//---------------------------------------------------------------------------
#ifndef GPOPT_CDecorrelator_H
#define GPOPT_CDecorrelator_H

#include "gpos/base.h"
#include "gpopt/operators/CExpression.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDecorrelator
	//
	//	@doc:
	//		Helper class for extracting correlated expressions
	//
	//---------------------------------------------------------------------------
	class CDecorrelator
	{

		private:

			// definition of operator processor
			typedef BOOL(FnProcessor)(IMemoryPool *, CExpression *, BOOL, CExpression **, DrgPexpr *);

			//---------------------------------------------------------------------------
			//	@struct:
			//		SOperatorProcessor
			//
			//	@doc:
			//		Mapping of operator to a processor function
			//
			//---------------------------------------------------------------------------
			struct SOperatorProcessor
			{
				// scalar operator id
				COperator::EOperatorId m_eopid;

				// pointer to handler function
				FnProcessor *m_pfnp;

			}; // struct SOperatorHandler

			// array of mappings
			static
			const SOperatorProcessor m_rgopproc[];

			// private ctor
			CDecorrelator();

			// private dtor
			virtual
			~CDecorrelator();

			// private copy ctor
			CDecorrelator(const CDecorrelator &);
			
			// helper to check if correlations below join are valid to be pulled-up
			static
			BOOL FPullableCorrelations
				(
				IMemoryPool *pmp,
				CExpression *pexpr,
				DrgPexpr *pdrgpexpr,
				DrgPexpr *pdrgpexprCorrelations
				);

			// check if scalar operator can be delayed
			static
			BOOL FDelayableScalarOp(CExpression *pexprScalar);

			// check if scalar expression can be lifted
			static 
			BOOL FDelayable(CExpression *pexprLogical, CExpression *pexprScalar, BOOL fEqualityOnly);
		
			// switch function for all operators
			static
			BOOL FProcessOperator
				(
				IMemoryPool *pmp,
				CExpression *pexpr,
				BOOL fEqualityOnly,
				CExpression **ppexprDecorrelated,
				DrgPexpr *pdrgpexprCorrelations
				);
				
			// processor for predicates
			static
			BOOL FProcessPredicate
				(
				IMemoryPool *pmp,
				CExpression *pexprLogical,
				CExpression *pexprScalar,
				BOOL fEqualityOnly,
				CColRefSet *pcrsOutput,
				CExpression **ppexprDecorrelated,
				DrgPexpr *pdrgpexprCorrelations
				);
			
			// processor for select operators
			static
			BOOL FProcessSelect
				(
				IMemoryPool *pmp,
				CExpression *pexpr,
				BOOL fEqualityOnly,
				CExpression **ppexprDecorrelated,
				DrgPexpr *pdrgpexprCorrelations
				);

		
			// processor for aggregates
			static
			BOOL FProcessGbAgg
				(
				IMemoryPool *pmp,
				CExpression *pexpr,
				BOOL fEqualityOnly,
				CExpression **ppexprDecorrelated,
				DrgPexpr *pdrgpexprCorrelations
				);

			// processor for joins (inner/n-ary)
			static
			BOOL FProcessJoin
				(
				IMemoryPool *pmp,
				CExpression *pexpr,
				BOOL fEqualityOnly,
				CExpression **ppexprDecorrelated,
				DrgPexpr *pdrgpexprCorrelations
				);


			// processor for projects
			static
			BOOL FProcessProject
				(
				IMemoryPool *pmp,
				CExpression *pexpr,
				BOOL fEqualityOnly,
				CExpression **ppexprDecorrelated,
				DrgPexpr *pdrgpexprCorrelations
				);
		
			// processor for assert
			static
			BOOL FProcessAssert
				(
				IMemoryPool *pmp,
				CExpression *pexpr,
				BOOL fEqualityOnly,
				CExpression **ppexprDecorrelated,
				DrgPexpr *pdrgpexprCorrelations
				);

			// processor for MaxOneRow
			static
			BOOL FProcessMaxOneRow
				(
				IMemoryPool *pmp,
				CExpression *pexpr,
				BOOL fEqualityOnly,
				CExpression **ppexprDecorrelated,
				DrgPexpr *pdrgpexprCorrelations
				);

			// processor for limits
			static
			BOOL FProcessLimit
				(
				IMemoryPool *pmp,
				CExpression *pexpr,
				BOOL fEqualityOnly,
				CExpression **ppexprDecorrelated,
				DrgPexpr *pdrgpexprCorrelations
				);

		public:

			// main handler
			static
			BOOL FProcess
				(
				IMemoryPool *pmp,
				CExpression *pexprOrig,
				BOOL fEqualityOnly,
				CExpression **ppexprDecorrelated,
				DrgPexpr *pdrgpexprCorrelations
				);

	}; // class CDecorrelator

}

#endif // !GPOPT_CDecorrelator_H

// EOF
