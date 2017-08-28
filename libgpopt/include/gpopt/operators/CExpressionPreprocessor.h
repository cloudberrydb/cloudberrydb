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
			typedef CHashMap<ULONG, DrgPexpr, gpos::UlHash<ULONG>, gpos::FEqual<ULONG>,
						CleanupDelete<ULONG>, CleanupRelease<DrgPexpr> > CTEPredsMap;

			// iterator for map of CTE id to collected predicates
			typedef CHashMapIter<ULONG, DrgPexpr, gpos::UlHash<ULONG>, gpos::FEqual<ULONG>,
						CleanupDelete<ULONG>, CleanupRelease<DrgPexpr> > CTEPredsMapIter;

			// generate a conjunction of equality predicates between the columns in the given set
			static
			CExpression *PexprConjEqualityPredicates(IMemoryPool *pmp, CColRefSet *pcrs);

			// additional equality predicates are generated based on the equivalence
			// classes in the constraint properties of the expression
			static
			CExpression *PexprAddEqualityPreds(IMemoryPool *pmp, CExpression *pexpr, CColRefSet *pcrsProcessed);

			// check if all columns in the given equivalence class come from one of the
			// children of the given expression
			static
			BOOL FEquivClassFromChild(CColRefSet *pcrs, CExpression *pexpr);

			// generate predicates for the given set of columns based on the given
			// constraint property
			static
			CExpression *PexprScalarPredicates
							(
							IMemoryPool *pmp,
							CPropConstraint *ppc,
							CColRefSet *pcrsNotNull,
							CColRefSet *pcrs,
							CColRefSet *pcrsProcessed
							);

			// eliminate self comparisons
			static
			CExpression *PexprEliminateSelfComparison(IMemoryPool *pmp, CExpression *pexpr);

			// remove CTE Anchor nodes
			static
			CExpression *PexprRemoveCTEAnchors(IMemoryPool *pmp, CExpression *pexpr);

			// trim superfluos equality
			static
			CExpression *PexprPruneSuperfluousEquality(IMemoryPool *pmp, CExpression *pexpr);

			// trim existential subqueries
			static
			CExpression *PexprTrimExistentialSubqueries(IMemoryPool *pmp, CExpression *pexpr);

			// simplify quantified subqueries
			static
			CExpression *PexprSimplifyQuantifiedSubqueries(IMemoryPool *pmp, CExpression *pexpr);

			// preliminary unnesting of scalar  subqueries
			static
			CExpression *PexprUnnestScalarSubqueries(IMemoryPool *pmp, CExpression *pexpr);

			// remove superfluous limit nodes
			static
			CExpression *PexprRemoveSuperfluousLimit(IMemoryPool *pmp, CExpression *pexpr);

			// remove superfluous outer references from limit, group by and window operators
			static
			CExpression *PexprRemoveSuperfluousOuterRefs(IMemoryPool *pmp, CExpression *pexpr);

			// generate predicates based on derived constraint properties
			static
			CExpression *PexprFromConstraints(IMemoryPool *pmp, CExpression *pexpr, CColRefSet *pcrsProcessed);

			// generate predicates based on derived constraint properties under scalar expressions
			static
			CExpression *PexprFromConstraintsScalar(IMemoryPool *pmp, CExpression *pexpr);

			// eliminate subtrees that have zero output cardinality
			static
			CExpression *PexprPruneEmptySubtrees(IMemoryPool *pmp, CExpression *pexpr);

			// collapse cascaded inner joins into NAry-joins
			static
			CExpression *PexprCollapseInnerJoins(IMemoryPool *pmp, CExpression *pexpr);

			// collapse cascaded logical project operators
			static
			CExpression *PexprCollapseProjects(IMemoryPool *pmp, CExpression *pexpr);

			// add dummy project element below scalar subquery when the output column is an outer reference
			static
			CExpression *PexprProjBelowSubquery(IMemoryPool *pmp, CExpression *pexpr, BOOL fUnderPrList);

			// helper function to rewrite IN query to simple EXISTS with a predicate
			static
			CExpression *ConvertInToSimpleExists (IMemoryPool *pmp, CExpression *pexpr);

			// rewrite IN subquery to EXIST subquery with a predicate
			static
			CExpression *PexprExistWithPredFromINSubq(IMemoryPool *pmp, CExpression *pexpr);

			// collapse cascaded union/union all into an NAry union/union all operator
			static
			CExpression *PexprCollapseUnionUnionAll(IMemoryPool *pmp, CExpression *pexpr);

			// transform outer joins into inner joins whenever possible
			static
			CExpression *PexprOuterJoinToInnerJoin(IMemoryPool *pmp, CExpression *pexpr);

			// eliminate CTE Anchors for CTEs that have zero consumers
			static
			CExpression *PexprRemoveUnusedCTEs(IMemoryPool *pmp, CExpression *pexpr);

			// collect CTE predicates from consumers
			static
			void CollectCTEPredicates(IMemoryPool *pmp, CExpression *pexpr, CTEPredsMap *phm);

			// imply new predicates on LOJ's inner child based on constraints derived from LOJ's outer child and join predicate
			static
			CExpression *PexprWithImpliedPredsOnLOJInnerChild(IMemoryPool *pmp, CExpression *pexprLOJ, BOOL *pfAddedPredicates);

			// infer predicate from outer child to inner child of the outer join
			static
			CExpression *PexprOuterJoinInferPredsFromOuterChildToInnerChild(IMemoryPool *pmp, CExpression *pexpr, BOOL *pfAddedPredicates);

			// driver for inferring predicates from constraints
			static
			CExpression *PexprInferPredicates(IMemoryPool *pmp, CExpression *pexpr);

			// entry for pruning unused computed columns
			static CExpression *
			PexprPruneUnusedComputedCols
				(
				IMemoryPool *pmp,
				CExpression *pexpr,
				CColRefSet *pcrsReqd
				);

			// driver for pruning unused computed columns
			static CExpression *
			PexprPruneUnusedComputedColsRecursive
				(
				IMemoryPool *pmp,
				CExpression *pexpr,
				CColRefSet *pcrsReqd
				);

			// prune unused project elements from the project list of Project or GbAgg
			static CExpression *
			PexprPruneProjListProjectOrGbAgg
				(
				IMemoryPool *pmp,
				CExpression *pexpr,
				CColRefSet *pcrsUnused,
				CColRefSet *pcrsDefined,
				const CColRefSet *pcrsReqd
				);

			// generate a scalar bool op expression or return the only child expression in array
			static CExpression *
			PexprScalarBoolOpConvert2In(IMemoryPool *pmp, CScalarBoolOp::EBoolOperator eboolop, DrgPexpr *pdrgpexpr);

			// determines if the expression is likely convertible to an array expression
			static BOOL
			FConvert2InIsConvertable(CExpression *pexpr, CScalarBoolOp::EBoolOperator eboolop);

			// reorder the scalar cmp children to ensure that left child is Scalar Ident and right Child is Scalar Const
			static CExpression *
			PexprReorderScalarCmpChildren(IMemoryPool *pmp, CExpression *pexpr);

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
			CExpression *PexprPreprocess(IMemoryPool *pmp, CExpression *pexpr, CColRefSet *pcrsOutputAndOrderCols = NULL);

			// add predicates collected from CTE consumers to producer expressions
			static
			void AddPredsToCTEProducers(IMemoryPool *pmp, CExpression *pexpr);

			// derive constraints on given expression
			static
			CExpression *PexprAddPredicatesFromConstraints(IMemoryPool *pmp, CExpression *pexpr);

			// convert series of AND or OR comparisons into array IN expressions
			static
			CExpression *PexprConvert2In(IMemoryPool *pmp, CExpression *pexpr);

	}; // class CExpressionPreprocessor
}


#endif // !GPOPT_CExpressionPreprocessor_H

// EOF
