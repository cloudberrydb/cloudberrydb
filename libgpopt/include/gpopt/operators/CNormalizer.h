//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright 2012 EMC Corp.
//
//	@filename:
//		CNormalizer.h
//
//	@doc:
//		Normalization of expression trees;
//		this currently includes predicates push down
//---------------------------------------------------------------------------
#ifndef GPOPT_CNormalizer_H
#define GPOPT_CNormalizer_H

#include "gpos/base.h"
#include "gpopt/operators/CExpression.h"

namespace gpopt
{

	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CNormalizer
	//
	//	@doc:
	//		Normalization of expression trees
	//
	//---------------------------------------------------------------------------
	class CNormalizer
	{

		private:

			// definition of push through function
			typedef void(FnPushThru)
				(
				IMemoryPool *mp,
				CExpression *pexprLogical,
				CExpression *pexprConj,
				CExpression **ppexprResult
				);

			//---------------------------------------------------------------------------
			//	@struct:
			//		SPushThru
			//
			//	@doc:
			//		Mapping of a logical operator to a push through function
			//
			//---------------------------------------------------------------------------
			struct SPushThru
			{
				// logical operator id
				COperator::EOperatorId m_eopid;

				// pointer to push through function
				FnPushThru *m_pfnpt;

			}; // struct SPushThru

			// array of mappings
			static
			const SPushThru m_rgpt[];

			//  return true if second expression is a child of first expression
			static
			BOOL FChild(CExpression *pexpr, CExpression *pexprChild);

			// simplify outer joins
			static
			BOOL FSimplifySelectOnOuterJoin(IMemoryPool *mp, CExpression *pexprOuterJoin, CExpression *pexprPred, CExpression **ppexprResult);

			static
			BOOL FSimplifySelectOnFullJoin(IMemoryPool *mp, CExpression *pexprFullJoin, CExpression *pexprPred, CExpression **ppexprResult);

			// call normalizer recursively on expression children
			static
			CExpression *PexprRecursiveNormalize(IMemoryPool *mp, CExpression *pexpr);

			// check if a scalar predicate can be pushed through a logical expression
			static
			BOOL FPushable(CExpression *pexprLogical, CExpression *pexprPred);

			// check if a scalar predicate can be pushed through the child of a sequence project expression
			static
			BOOL FPushableThruSeqPrjChild(CExpression *pexprSeqPrj, CExpression *pexprPred);

			// check if a conjunct should be pushed through expression's outer child
			static
			BOOL FPushThruOuterChild(CExpression *pexprLogical);

			// return a Select expression, if needed, with a scalar condition made of given array of conjuncts
			static
			CExpression *PexprSelect(IMemoryPool *mp, CExpression *pexpr, CExpressionArray *pdrgpexpr);

			// push scalar expression through an expression with unary operator with scalar child
			static
			void PushThruUnaryWithScalarChild(IMemoryPool *mp, CExpression *pexprLogical, CExpression *pexprConj, CExpression **ppexprResult);

			// push scalar expression through a sequence project expression
			static
			void PushThruSeqPrj(IMemoryPool *mp, CExpression *pexprSeqPrj, CExpression *pexprConj, CExpression **ppexprResult);

			// push scalar expression through a set operation
			static
			void PushThruSetOp(IMemoryPool *mp, CExpression *pexprSetOp, CExpression *pexprConj, CExpression **ppexprResult);

			// push a conjunct through a CTE anchor operator
			static
			void PushThruUnaryWithoutScalarChild(IMemoryPool *mp, CExpression *pexprLogical, CExpression *pexprConj, CExpression **ppexprResult);

			// push scalar expression through a select expression
			static
			void PushThruSelect(IMemoryPool *mp, CExpression *pexprSelect, CExpression *pexprConj, CExpression **ppexprResult);

			// split the given conjunct into pushable and unpushable predicates
			static
			void SplitConjunct(IMemoryPool *mp, CExpression *pexpr, CExpression *pexprConj, CExpressionArray **ppdrgpexprPushable, CExpressionArray **ppdrgpexprUnpushable);

			// split the given conjunct into pushable and unpushable predicates for a sequence project expression
			static
			void SplitConjunctForSeqPrj(IMemoryPool *mp, CExpression *pexprSeqPrj, CExpression *pexprConj, CExpressionArray **ppdrgpexprPushable, CExpressionArray **ppdrgpexprUnpushable);

			// push scalar expression through left outer join children
			static
			void PushThruOuterChild(IMemoryPool *mp, CExpression *pexpr, CExpression *pexprConj, CExpression **ppexprResult);

			// push scalar expression through a join expression
			static
			void PushThruJoin(IMemoryPool *mp, CExpression *pexprJoin, CExpression *pexprConj, CExpression **ppexprResult);

			// push scalar expression through logical expression
			static
			void PushThru(IMemoryPool *mp, CExpression *pexprLogical, CExpression *pexprConj, CExpression **ppexprResult);

			// push an array of conjuncts through logical expression, and compute an array of remaining conjuncts
			static
			void PushThru
				(
				IMemoryPool *mp,
				CExpression *pexprLogical,
				CExpressionArray *pdrgpexprConjuncts,
				CExpression **ppexprResult,
				CExpressionArray **ppdrgpexprRemaining
				);

			// private copy ctor
			CNormalizer(const CNormalizer &);

			// pull logical projects as far up the logical tree as possible, and
			// combine consecutive projects if possible
			static
			CExpression *PexprPullUpAndCombineProjects(IMemoryPool *mp, CExpression *pexpr, BOOL *pfSuccess);

			// pull up project elements from the given projection expression that do not
			// exist in the given used columns set
			static
			CExpression *PexprPullUpProjectElements
				(
				IMemoryPool *mp,
				CExpression *pexpr,
				CColRefSet *pcrsUsed,
				CColRefSet *pcrsOutput,
				CExpressionArray **ppdrgpexprPrElPullUp
				);

#ifdef GPOS_DEBUG
			// check if the columns used by the operator are a subset of its input columns
			static
			BOOL FLocalColsSubsetOfInputCols(IMemoryPool *mp, CExpression *pexpr);
#endif //GPOS_DEBUG

		public:

			// main driver
			static
			CExpression *PexprNormalize(IMemoryPool *mp, CExpression *pexpr);

			// normalize joins so that they are on colrefs
			static
			CExpression *PexprNormalizeJoins(IMemoryPool *mp, CExpression *pexpr);

			// pull logical projects as far up the logical tree as possible, and
			// combine consecutive projects if possible
			static
			CExpression *PexprPullUpProjections(IMemoryPool *mp, CExpression *pexpr);

	}; // class CNormalizer
}


#endif // !GPOPT_CNormalizer_H

// EOF
