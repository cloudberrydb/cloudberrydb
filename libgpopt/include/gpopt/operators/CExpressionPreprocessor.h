//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright 2012 EMC Corp.
//
//	@filename:
//		CExpressionPreprocessor.h
//
//	@doc:
//		Expression tree preprocessing routines, needed to prepare an input
//		logical expression to be optimized
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CExpressionPreprocessor_H
#define GPOPT_CExpressionPreprocessor_H

#include "gpos/base.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/base/CColumnFactory.h"

#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CScalarBoolOp.h"
#include "gpopt/mdcache/CMDAccessor.h"

namespace gpopt
{

	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CExpressionPreprocessor
	//
	//	@doc:
	//		Expression preprocessing routines
	//
	//---------------------------------------------------------------------------
	class CExpressionPreprocessor
	{

		private:

			// map CTE id to collected predicates
			typedef CHashMap<ULONG, CExpressionArray, gpos::HashValue<ULONG>, gpos::Equals<ULONG>,
						CleanupDelete<ULONG>, CleanupRelease<CExpressionArray> > CTEPredsMap;

			// iterator for map of CTE id to collected predicates
			typedef CHashMapIter<ULONG, CExpressionArray, gpos::HashValue<ULONG>, gpos::Equals<ULONG>,
						CleanupDelete<ULONG>, CleanupRelease<CExpressionArray> > CTEPredsMapIter;

			// generate a conjunction of equality predicates between the columns in the given set
			static
			CExpression *PexprConjEqualityPredicates(IMemoryPool *mp, CColRefSet *pcrs);

			// additional equality predicates are generated based on the equivalence
			// classes in the constraint properties of the expression
			static
			CExpression *PexprAddEqualityPreds(IMemoryPool *mp, CExpression *pexpr, CColRefSet *pcrsProcessed);

			// check if all columns in the given equivalence class come from one of the
			// children of the given expression
			static
			BOOL FEquivClassFromChild(CColRefSet *pcrs, CExpression *pexpr);

			// generate predicates for the given set of columns based on the given
			// constraint property
			static
			CExpression *PexprScalarPredicates
							(
							IMemoryPool *mp,
							CPropConstraint *ppc,
							CColRefSet *pcrsNotNull,
							CColRefSet *pcrs,
							CColRefSet *pcrsProcessed
							);

			// eliminate self comparisons
			static
			CExpression *PexprEliminateSelfComparison(IMemoryPool *mp, CExpression *pexpr);

			// remove CTE Anchor nodes
			static
			CExpression *PexprRemoveCTEAnchors(IMemoryPool *mp, CExpression *pexpr);

			// trim superfluos equality
			static
			CExpression *PexprPruneSuperfluousEquality(IMemoryPool *mp, CExpression *pexpr);

			// trim existential subqueries
			static
			CExpression *PexprTrimExistentialSubqueries(IMemoryPool *mp, CExpression *pexpr);

			// simplify quantified subqueries
			static
			CExpression *PexprSimplifyQuantifiedSubqueries(IMemoryPool *mp, CExpression *pexpr);

			// preliminary unnesting of scalar  subqueries
			static
			CExpression *PexprUnnestScalarSubqueries(IMemoryPool *mp, CExpression *pexpr);

			// remove superfluous limit nodes
			static
			CExpression *PexprRemoveSuperfluousLimit(IMemoryPool *mp, CExpression *pexpr);

			// remove superfluous outer references from limit, group by and window operators
			static
			CExpression *PexprRemoveSuperfluousOuterRefs(IMemoryPool *mp, CExpression *pexpr);

			// generate predicates based on derived constraint properties
			static
			CExpression *PexprFromConstraints(IMemoryPool *mp, CExpression *pexpr, CColRefSet *pcrsProcessed);

			// generate predicates based on derived constraint properties under scalar expressions
			static
			CExpression *PexprFromConstraintsScalar(IMemoryPool *mp, CExpression *pexpr);

			// eliminate subtrees that have zero output cardinality
			static
			CExpression *PexprPruneEmptySubtrees(IMemoryPool *mp, CExpression *pexpr);

			// collapse cascaded inner joins into NAry-joins
			static
			CExpression *PexprCollapseInnerJoins(IMemoryPool *mp, CExpression *pexpr);

			// collapse cascaded logical project operators
			static
			CExpression *PexprCollapseProjects(IMemoryPool *mp, CExpression *pexpr);

			// add dummy project element below scalar subquery when the output column is an outer reference
			static
			CExpression *PexprProjBelowSubquery(IMemoryPool *mp, CExpression *pexpr, BOOL fUnderPrList);

			// helper function to rewrite IN query to simple EXISTS with a predicate
			static
			CExpression *ConvertInToSimpleExists (IMemoryPool *mp, CExpression *pexpr);

			// rewrite IN subquery to EXIST subquery with a predicate
			static
			CExpression *PexprExistWithPredFromINSubq(IMemoryPool *mp, CExpression *pexpr);

			// collapse cascaded union/union all into an NAry union/union all operator
			static
			CExpression *PexprCollapseUnionUnionAll(IMemoryPool *mp, CExpression *pexpr);

			// transform outer joins into inner joins whenever possible
			static
			CExpression *PexprOuterJoinToInnerJoin(IMemoryPool *mp, CExpression *pexpr);

			// eliminate CTE Anchors for CTEs that have zero consumers
			static
			CExpression *PexprRemoveUnusedCTEs(IMemoryPool *mp, CExpression *pexpr);

			// collect CTE predicates from consumers
			static
			void CollectCTEPredicates(IMemoryPool *mp, CExpression *pexpr, CTEPredsMap *phm);

			// imply new predicates on LOJ's inner child based on constraints derived from LOJ's outer child and join predicate
			static
			CExpression *PexprWithImpliedPredsOnLOJInnerChild(IMemoryPool *mp, CExpression *pexprLOJ, BOOL *pfAddedPredicates);

			// infer predicate from outer child to inner child of the outer join
			static
			CExpression *PexprOuterJoinInferPredsFromOuterChildToInnerChild(IMemoryPool *mp, CExpression *pexpr, BOOL *pfAddedPredicates);

			// driver for inferring predicates from constraints
			static
			CExpression *PexprInferPredicates(IMemoryPool *mp, CExpression *pexpr);

			// entry for pruning unused computed columns
			static CExpression *
			PexprPruneUnusedComputedCols
				(
				IMemoryPool *mp,
				CExpression *pexpr,
				CColRefSet *pcrsReqd
				);

			// driver for pruning unused computed columns
			static CExpression *
			PexprPruneUnusedComputedColsRecursive
				(
				IMemoryPool *mp,
				CExpression *pexpr,
				CColRefSet *pcrsReqd
				);

			// prune unused project elements from the project list of Project or GbAgg
			static CExpression *
			PexprPruneProjListProjectOrGbAgg
				(
				IMemoryPool *mp,
				CExpression *pexpr,
				CColRefSet *pcrsUnused,
				CColRefSet *pcrsDefined,
				const CColRefSet *pcrsReqd
				);

			// generate a scalar bool op expression or return the only child expression in array
			static CExpression *
			PexprScalarBoolOpConvert2In(IMemoryPool *mp, CScalarBoolOp::EBoolOperator eboolop, CExpressionArray *pdrgpexpr);

			// determines if the expression is likely convertible to an array expression
			static BOOL
			FConvert2InIsConvertable(CExpression *pexpr, CScalarBoolOp::EBoolOperator eboolop);

			// reorder the scalar cmp children to ensure that left child is Scalar Ident and right Child is Scalar Const
			static CExpression *
			PexprReorderScalarCmpChildren(IMemoryPool *mp, CExpression *pexpr);

			// private ctor
			CExpressionPreprocessor();

			// private dtor
			virtual
			~CExpressionPreprocessor();

			// private copy ctor
			CExpressionPreprocessor(const CExpressionPreprocessor &);

		public:

			// main driver
			static
			CExpression *PexprPreprocess(IMemoryPool *mp, CExpression *pexpr, CColRefSet *pcrsOutputAndOrderCols = NULL);

			// add predicates collected from CTE consumers to producer expressions
			static
			void AddPredsToCTEProducers(IMemoryPool *mp, CExpression *pexpr);

			// derive constraints on given expression
			static
			CExpression *PexprAddPredicatesFromConstraints(IMemoryPool *mp, CExpression *pexpr);

			// convert series of AND or OR comparisons into array IN expressions
			static
			CExpression *PexprConvert2In(IMemoryPool *mp, CExpression *pexpr);

	}; // class CExpressionPreprocessor
}


#endif // !GPOPT_CExpressionPreprocessor_H

// EOF
