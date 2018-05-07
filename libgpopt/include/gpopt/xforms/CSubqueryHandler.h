//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CSubqueryHandler.h
//
//	@doc:
//		Helper class for transforming subquery expressions to Apply
//		expressions
//---------------------------------------------------------------------------
#ifndef GPOPT_CSubqueryHandler_H
#define GPOPT_CSubqueryHandler_H

#include "gpos/base.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CSubqueryHandler
	//
	//	@doc:
	//		Helper class for transforming subquery expressions to Apply
	//		expressions
	//
	//---------------------------------------------------------------------------
	class CSubqueryHandler
	{

		public:

			// context in which subquery appears
			enum ESubqueryCtxt
			{
				EsqctxtValue,		// subquery appears in a project list
				EsqctxtFilter		// subquery appears in a comparison predicate
			};

		private:

			//---------------------------------------------------------------------------
			//	@struct:
			//		SSubqueryDesc
			//
			//	@doc:
			//		Structure to maintain subquery descriptor
			//
			//---------------------------------------------------------------------------
			struct SSubqueryDesc
			{
				// subquery can return more than one row
				BOOL m_fReturnSet;

				// subquery has volatile functions
				BOOL m_fHasVolatileFunctions;

				// subquery has outer references
				BOOL m_fHasOuterRefs;

				// subquery has skip level correlations -- when inner expression refers to columns defined above the immediate outer expression
				BOOL m_fHasSkipLevelCorrelations;

				// subquery has a single count(*)/count(Any) agg
				BOOL m_fHasCountAgg;

				// column defining count(*)/count(Any) agg, if any
				CColRef *m_pcrCountAgg;

				//  does subquery project a count expression
				BOOL m_fProjectCount;

				// subquery is used in a value context
				BOOL m_fValueSubquery;

				// subquery requires correlated execution
				BOOL m_fCorrelatedExecution;

				// ctor
				SSubqueryDesc()
					:
					m_fReturnSet(false),
					m_fHasVolatileFunctions(false),
					m_fHasOuterRefs(false),
					m_fHasSkipLevelCorrelations(false),
					m_fHasCountAgg(false),
					m_pcrCountAgg(NULL),
					m_fProjectCount(false),
					m_fValueSubquery(false),
					m_fCorrelatedExecution(false)
				{}

				// set correlated execution flag
				void SetCorrelatedExecution();

			}; // struct SSubqueryDesc

			// memory pool
			IMemoryPool *m_pmp;

			// enforce using correlated apply for unnesting subqueries
			BOOL m_fEnforceCorrelatedApply;

			// private copy ctor
			CSubqueryHandler(const CSubqueryHandler &);

			// helper for adding nullness check, only if needed, to the given scalar expression
			static
			CExpression *PexprIsNotNull(IMemoryPool *pmp, CExpression *pexprOuter, CExpression *pexprLogical, CExpression *pexprScalar);

			// helper for adding a Project node with a const TRUE on top of the given expression
			static
			void AddProjectNode
				(
				IMemoryPool *pmp,
				CExpression *pexpr,
				CExpression *pexprSubquery,
				CExpression **ppexprResult
				);

			// helper for creating an inner select expression when creating outer apply
			static
			CExpression *PexprInnerSelect
				(
				IMemoryPool *pmp,
				const CColRef *pcrInner,
				CExpression *pexprInner,
				CExpression *pexprPredicate
				);

			// helper for creating outer apply expression for scalar subqueries
			static
			BOOL FCreateOuterApplyForScalarSubquery
				(
				IMemoryPool *pmp,
				CExpression *pexprOuter,
				CExpression *pexprInner,
				CExpression *pexprSubquery,
				BOOL fOuterRefsUnderInner,
				CExpression **ppexprNewOuter,
				CExpression **ppexprResidualScalar
				);

			// helper for creating grouping columns for outer apply expression
			static
			BOOL FCreateGrpCols
				(
				IMemoryPool *pmp,
				CExpression *pexprOuter,
				CExpression *pexprInner,
				BOOL fExistential,
				BOOL fOuterRefsUnderInner,
				DrgPcr **ppdrgpcr, // output: constructed grouping columns
				BOOL *pfGbOnInner // output: is Gb created on inner expression
				);

			// helper for creating outer apply expression for existential/quantified subqueries
			static
			BOOL FCreateOuterApplyForExistOrQuant
				(
				IMemoryPool *pmp,
				CExpression *pexprOuter,
				CExpression *pexprInner,
				CExpression *pexprSubquery,
				BOOL fOuterRefsUnderInner,
				CExpression **ppexprNewOuter,
				CExpression **ppexprResidualScalar
				);

			// helper for creating outer apply expression
			static
			BOOL FCreateOuterApply
				(
				IMemoryPool *pmp,
				CExpression *pexprOuter,
				CExpression *pexprInner,
				CExpression *pexprSubquery,
				BOOL fOuterRefsUnderInner,
				CExpression **ppexprNewOuter,
				CExpression **ppexprResidualScalar
				);

			// helper for creating a scalar if expression used when generating an outer apply
			static
			CExpression *PexprScalarIf(IMemoryPool *pmp, CColRef *pcrBool, CColRef *pcrSum, CColRef *pcrCount, CExpression *pexprSubquery);

			// helper for creating a correlated apply expression for existential subquery
			static
			BOOL FCreateCorrelatedApplyForExistentialSubquery
				(
				IMemoryPool *pmp,
				CExpression *pexprOuter,
				CExpression *pexprSubquery,
				ESubqueryCtxt esqctxt,
				CExpression **ppexprNewOuter,
				CExpression **ppexprResidualScalar
				);

			// helper for creating a correlated apply expression for quantified subquery
			static
			BOOL FCreateCorrelatedApplyForQuantifiedSubquery
				(
				IMemoryPool *pmp,
				CExpression *pexprOuter,
				CExpression *pexprSubquery,
				ESubqueryCtxt esqctxt,
				CExpression **ppexprNewOuter,
				CExpression **ppexprResidualScalar
				);

			// helper for creating correlated apply expression
			static
			BOOL FCreateCorrelatedApplyForExistOrQuant
				(
				IMemoryPool *pmp,
				CExpression *pexprOuter,
				CExpression *pexprSubquery,
				ESubqueryCtxt esqctxt,
				CExpression **ppexprNewOuter,
				CExpression **ppexprResidualScalar
				);

			// create subquery descriptor
			static
			SSubqueryDesc *Psd(IMemoryPool *pmp, CExpression *pexprSubquery, CExpression *pexprOuter, ESubqueryCtxt esqctxt);

			// detect subqueries with expressions over count aggregate similar to
			// (SELECT 'abc' || (SELECT count(*) from X))
			static
			BOOL FProjectCountSubquery(CExpression *pexprSubquery, CColRef *ppcrCount);

			// given an input expression, replace all occurrences of given column with the given scalar expression
			static
			CExpression *PexprReplace
				(
				IMemoryPool *pmp,
				CExpression *pexpr,
				CColRef *pcr,
				CExpression *pexprSubquery
				);

			// remove a scalar subquery node from scalar tree
			BOOL FRemoveScalarSubquery
				(
				CExpression *pexprOuter,
				CExpression *pexprSubquery,
				ESubqueryCtxt esqctxt,
				CExpression **ppexprNewOuter,
				CExpression **ppexprResidualScalar
				);

			// helper to generate a correlated apply expression when needed
			static
			BOOL FGenerateCorrelatedApplyForScalarSubquery
				(
				IMemoryPool *pmp,
				CExpression *pexprOuter,
				CExpression *pexprSubquery,
				ESubqueryCtxt esqctxt,
				CSubqueryHandler::SSubqueryDesc *psd,
				BOOL fEnforceCorrelatedApply,
				CExpression **ppexprNewOuter,
				CExpression **ppexprResidualScalar
				);

			// internal function for removing a scalar subquery node from scalar tree
			static
			BOOL FRemoveScalarSubqueryInternal
				(
				IMemoryPool *pmp,
				CExpression *pexprOuter,
				CExpression *pexprSubquery,
				ESubqueryCtxt esqctxt,
				SSubqueryDesc *psd,
				BOOL fEnforceCorrelatedApply,
				CExpression **ppexprNewOuter,
				CExpression **ppexprResidualScalar
				);

			// remove a subquery ANY node from scalar tree
			BOOL FRemoveAnySubquery
				(
				CExpression *pexprOuter,
				CExpression *pexprSubquery,
				ESubqueryCtxt esqctxt,
				CExpression **ppexprNewOuter,
				CExpression **ppexprResidualScalar
				);

			// remove a subquery ALL node from scalar tree
			BOOL FRemoveAllSubquery
				(
				CExpression *pexprOuter,
				CExpression *pexprSubquery,
				ESubqueryCtxt esqctxt,
				CExpression **ppexprNewOuter,
				CExpression **ppexprResidualScalar
				);

			// remove a subquery EXISTS/NOT EXISTS node from scalar tree
			static
			BOOL FRemoveExistentialSubquery
				(
				IMemoryPool *pmp,
				COperator::EOperatorId eopid,
				CExpression *pexprOuter,
				CExpression *pexprSubquery,
				ESubqueryCtxt esqctxt,
				CExpression **ppexprNewOuter,
				CExpression **ppexprResidualScalar
				);

			// remove a subquery EXISTS from scalar tree
			BOOL FRemoveExistsSubquery
				(
				CExpression *pexprOuter,
				CExpression *pexprSubquery,
				ESubqueryCtxt esqctxt,
				CExpression **ppexprNewOuter,
				CExpression **ppexprResidualScalar
				);

			// remove a subquery NOT EXISTS from scalar tree
			BOOL FRemoveNotExistsSubquery
				(
				CExpression *pexprOuter,
				CExpression *pexprSubquery,
				ESubqueryCtxt esqctxt,
				CExpression **ppexprNewOuter,
				CExpression **ppexprResidualScalar
				);

			// handle subqueries in scalar tree recursively
			BOOL FRecursiveHandler
				(
				CExpression *pexprOuter,
				CExpression *pexprScalar,
				ESubqueryCtxt esqctxt,
				CExpression **ppexprNewOuter,
				CExpression **ppexprNewScalar
				);

			// handle subqueries on a case-by-case basis
			BOOL FProcessScalarOperator
				(
				CExpression *pexprOuter,
				CExpression *pexprScalar,
				ESubqueryCtxt esqctxt,
				CExpression **ppexprNewOuter,
				CExpression **ppexprNewScalar
				);

#ifdef GPOS_DEBUG
			// assert valid values of arguments
			static
			void AssertValidArguments
				(
				IMemoryPool *pmp,
				CExpression *pexprOuter,
				CExpression *pexprScalar,
				CExpression **ppexprNewOuter,
				CExpression **ppexprResidualScalar
				);
#endif // GPOS_DEBUG

		public:

			// ctor
			CSubqueryHandler
				(
				IMemoryPool *pmp,
				BOOL fEnforceCorrelatedApply
				)
				:
				m_pmp(pmp),
				m_fEnforceCorrelatedApply(fEnforceCorrelatedApply)
			{}

			// build an expression for the quantified comparison of the subquery
			CExpression *PexprSubqueryPred
				(
				CExpression *pexprOuter,
				CExpression *pexprSubquery,
				CExpression **ppexprResult
				);

			// main driver
			BOOL FProcess
				(
				CExpression *pexprOuter, // logical child of a SELECT node
				CExpression *pexprScalar, // scalar child of a SELECT node
				ESubqueryCtxt esqctxt,	// context in which subquery occurs
				CExpression **ppexprNewOuter, // an Apply logical expression produced as output
				CExpression **ppexprResidualScalar // residual scalar expression produced as output
				);

	}; // class CSubqueryHandler

}

#endif // !GPOPT_CSubqueryHandler_H

// EOF
