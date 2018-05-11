//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CSubqueryTestUtils.h
//
//	@doc:
//		Optimizer test utility functions for tests requiring subquery
//		expressions
//---------------------------------------------------------------------------
#ifndef GPOPT_CSubqueryTestUtils_H
#define GPOPT_CSubqueryTestUtils_H

#include "gpos/base.h"
#include "unittest/gpopt/CTestUtils.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CSubqueryTestUtils
	//
	//	@doc:
	//		Test utility functions for tests requiring subquery expressions
	//
	//---------------------------------------------------------------------------
	class CSubqueryTestUtils
	{

		public:

			//-------------------------------------------------------------------
			// Helpers for generating expressions
			//-------------------------------------------------------------------
						
			// helper to generate a pair of random Get expressions
			static
			void GenerateGetExpressions(IMemoryPool *mp, CExpression **ppexprOuter, CExpression **ppexprInner);

			// generate a random join expression with a subquery predicate
			static
			CExpression *PexprJoinWithAggSubquery(IMemoryPool *mp, BOOL fCorrelated);

			// generate a select  expression with subquery equality predicate
			static
			CExpression *PexprSelectWithAggSubquery(IMemoryPool *mp, CExpression *pexprOuter, CExpression *pexprInner, BOOL fCorrelated);

			// generate a Select expression with a subquery equality predicate involving constant
			static
			CExpression *PexprSelectWithAggSubqueryConstComparison(IMemoryPool *mp, CExpression *pexprOuter, CExpression *pexprInner, BOOL fCorrelated);

			// generate a project  expression with subquery equality predicate
			static
			CExpression *PexprProjectWithAggSubquery(IMemoryPool *mp, CExpression *pexprOuter, CExpression *pexprInner, BOOL fCorrelated);

			// generate a random select expression with a subquery predicate
			static
			CExpression *PexprSelectWithAggSubquery(IMemoryPool *mp, BOOL fCorrelated);

			// generate a random select expression with a subquery predicate involving constant
			static
			CExpression *PexprSelectWithAggSubqueryConstComparison(IMemoryPool *mp, BOOL fCorrelated);

			// generate a random project expression with a subquery in project element
			static
			CExpression *PexprProjectWithAggSubquery(IMemoryPool *mp, BOOL fCorrelated);

			// generate a random select expression with a subquery over join predicate
			static
			CExpression *PexprSelectWithAggSubqueryOverJoin(IMemoryPool *mp, BOOL fCorrelated);

			// generate a random select expression with ANY subquery predicate
			static
			CExpression *PexprSelectWithAnySubquery(IMemoryPool *mp, BOOL fCorrelated);

			// generate a random select expression with ANY subquery predicate over window operation
			static
			CExpression *PexprSelectWithAnySubqueryOverWindow(IMemoryPool *mp, BOOL fCorrelated);

			// generate a random select expression with Any subquery whose inner expression is a GbAgg
			static
			CExpression *PexprSelectWithAnyAggSubquery(IMemoryPool *mp, BOOL fCorrelated);

			// generate a random project expression with ANY subquery
			static
			CExpression *PexprProjectWithAnySubquery(IMemoryPool *mp,  BOOL fCorrelated);

			// generate a random select expression with ALL subquery predicate
			static
			CExpression *PexprSelectWithAllSubquery(IMemoryPool *mp, BOOL fCorrelated);

			// generate a random select expression with ALL subquery predicate over window operation
			static
			CExpression *PexprSelectWithAllSubqueryOverWindow(IMemoryPool *mp, BOOL fCorrelated);

			// generate a random select expression with Any subquery whose inner expression is a GbAgg
			static
			CExpression *PexprSelectWithAllAggSubquery(IMemoryPool *mp, BOOL fCorrelated);

			// generate a random project expression with ALL subquery
			static
			CExpression *PexprProjectWithAllSubquery(IMemoryPool *mp, BOOL fCorrelated);

			// generate a random select expression with EXISTS subquery predicate
			static
			CExpression *PexprSelectWithExistsSubquery(IMemoryPool *mp, BOOL fCorrelated);

			// generate a random select expression with trimmable EXISTS subquery predicate
			static
			CExpression *PexprSelectWithTrimmableExists(IMemoryPool *mp, BOOL fCorrelated);

			// generate a random select expression with trimmable NOT EXISTS subquery predicate
			static
			CExpression *PexprSelectWithTrimmableNotExists(IMemoryPool *mp, BOOL fCorrelated);

			// generate a random select expression with NOT EXISTS subquery predicate
			static
			CExpression *PexprSelectWithNotExistsSubquery(IMemoryPool *mp, BOOL fCorrelated);

			// generate a random project expression with EXISTS subquery
			static
			CExpression *PexprProjectWithExistsSubquery(IMemoryPool *mp, BOOL fCorrelated);

			// generate a random project expression with NOT EXISTS subquery
			static
			CExpression *PexprProjectWithNotExistsSubquery(IMemoryPool *mp, BOOL fCorrelated);

			// generate a random select expression with nested comparisons involving subqueries
			static
			CExpression *PexprSelectWithNestedCmpSubquery(IMemoryPool *mp, BOOL fCorrelated);

			// generate randomized select expression with comparison between two subqueries
			static
			CExpression *PexprSelectWithCmpSubqueries(IMemoryPool *mp, BOOL fCorrelated);

			// generate a random select expression with nested agg subqueries
			static
			CExpression *PexprSelectWithNestedSubquery(IMemoryPool *mp, BOOL fCorrelated);

			// generate a random select expression with nested All subqueries
			static
			CExpression *PexprSelectWithNestedAllSubqueries(IMemoryPool *mp, BOOL fCorrelated);

			// generate a random select expression with nested Any subqueries
			static
			CExpression *PexprSelectWithNestedAnySubqueries(IMemoryPool *mp, BOOL fCorrelated);

			// generate a random select expression with 2-levels correlated subqueries
			static
			CExpression *PexprSelectWith2LevelsCorrSubquery(IMemoryPool *mp, BOOL fCorrelated);

			// generate a random select expression with subquery predicates in an AND tree
			static
			CExpression *PexprSelectWithSubqueryConjuncts(IMemoryPool *mp, BOOL fCorrelated);

			// generate a random select expression with subquery predicates in an OR tree
			static
			CExpression *PexprSelectWithSubqueryDisjuncts(IMemoryPool *mp, BOOL fCorrelated);

			// generate a random project expression with subqueries
			static
			CExpression *PexprProjectWithSubqueries(IMemoryPool *mp, BOOL fCorrelated);

			// generate a randomized Select expression to be used for building subquery examples
			static
			CExpression *PexprSubquery(IMemoryPool *mp, CExpression *pexprOuter, CExpression *pexprInner, BOOL fCorrelated);

			// generate a quantified subquery expression
			static
			CExpression *PexprSubqueryQuantified(IMemoryPool *mp, COperator::EOperatorId op_id, CExpression *pexprOuter, CExpression *pexprInner, BOOL fCorrelated);

			// generate quantified subquery expression over window operations
			static
			CExpression *PexprSelectWithSubqueryQuantifiedOverWindow(IMemoryPool *mp, COperator::EOperatorId op_id, BOOL fCorrelated);

			// generate existential subquery expression
			static
			CExpression *PexprSubqueryExistential(IMemoryPool *mp, COperator::EOperatorId op_id, CExpression *pexprOuter, CExpression *pexprInner, BOOL fCorrelated);

			// generate a ScalarSubquery aggregate expression
			static
			CExpression *PexprSubqueryAgg(IMemoryPool *mp, CExpression *pexprOuter, CExpression *pexprInner, BOOL fCorrelated);

			// generate a random select expression with nested quantified subqueries
			static
			CExpression *PexprSelectWithNestedQuantifiedSubqueries(IMemoryPool *mp, COperator::EOperatorId op_id, BOOL fCorrelated);

			// generate randomized quantified subquery expression
			static
			CExpression *PexprSelectWithSubqueryQuantified(IMemoryPool *mp, COperator::EOperatorId op_id, BOOL fCorrelated);

			// generate randomized quantified subquery expression whose inner expression is a Gb
			static
			CExpression *PexprSelectWithQuantifiedAggSubquery(IMemoryPool *mp, COperator::EOperatorId op_id, BOOL fCorrelated);

			// generate randomized Project expression with quantified subquery
			static
			CExpression *PexprProjectWithSubqueryQuantified(IMemoryPool *mp, CExpression *pexprOuter, CExpression *pexprInner, COperator::EOperatorId op_id, BOOL fCorrelated);

			// generate randomized Select with existential subquery expression
			static
			CExpression *PexprSelectWithSubqueryExistential(IMemoryPool *mp, COperator::EOperatorId op_id, BOOL fCorrelated);

			// generate randomized Select expression with existential subquery predicate that can be trimmed
			static
			CExpression *PexprSelectWithTrimmableExistentialSubquery(IMemoryPool *mp, COperator::EOperatorId op_id, BOOL fCorrelated);

			// generate randomized Project with existential subquery expression
			static
			CExpression *PexprProjectWithSubqueryExistential(IMemoryPool *mp, COperator::EOperatorId op_id, BOOL fCorrelated);

			// generate a Select expression with an AND predicate tree involving subqueries
			static
			CExpression *PexprSelectWithSubqueryBoolOp(IMemoryPool *mp, CExpression *pexprOuter, CExpression *pexprInner, BOOL fCorrelated, CScalarBoolOp::EBoolOperator);

			// generate a Project expression with multiple subqueries
			static
			CExpression *PexprProjectWithSubqueries(IMemoryPool *mp, CExpression *pexprOuter, CExpression *pexprInner, BOOL fCorrelated);

			// generate a Select expression with ANY/ALL subquery predicate over a const table get
			static
			CExpression *PexprSubqueryWithConstTableGet(IMemoryPool *mp, COperator::EOperatorId op_id);

			// generate a Select expression with a disjunction of two ANY subqueries
			static
			CExpression *PexprSubqueryWithDisjunction(IMemoryPool *mp);

			// generate an expression with subquery in null test context
			static
			CExpression *PexprSubqueriesInNullTestContext(IMemoryPool *mp, BOOL fCorrelated);

			// generate an expression with subqueries in both value and filter contexts
			static
			CExpression *PexprSubqueriesInDifferentContexts(IMemoryPool *mp, BOOL fCorrelated);

			// generate an expression with undecorrelatable quantified subquery
			static
			CExpression *PexprUndecorrelatableSubquery(IMemoryPool *mp, COperator::EOperatorId op_id, BOOL fCorrelated);

			// generate an expression with undecorrelatable ANY subquery
			static
			CExpression *PexprUndecorrelatableAnySubquery(IMemoryPool *mp, BOOL fCorrelated);

			// generate an expression with undecorrelatable ALL subquery
			static
			CExpression *PexprUndecorrelatableAllSubquery(IMemoryPool *mp, BOOL fCorrelated);

			// generate an expression with undecorrelatable exists subquery
			static
			CExpression *PexprUndecorrelatableExistsSubquery(IMemoryPool *mp, BOOL fCorrelated);

			// generate an expression with undecorrelatable NotExists subquery
			static
			CExpression *PexprUndecorrelatableNotExistsSubquery(IMemoryPool *mp, BOOL fCorrelated);

			// generate an expression with undecorrelatable Scalar subquery
			static
			CExpression *PexprUndecorrelatableScalarSubquery(IMemoryPool *mp, BOOL fCorrelated);

	}; // class CSubqueryTestUtils

} // namespace gpopt

#endif // !GPOPT_CSubqueryTestUtils_H

// EOF
