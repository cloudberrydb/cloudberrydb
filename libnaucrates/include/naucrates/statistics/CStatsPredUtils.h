//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright 2014 Pivotal Inc.
//
//	@filename:
//		CStatsPredUtils.h
//
//	@doc:
//		Utility functions for extracting statistics predicates
//---------------------------------------------------------------------------
#ifndef GPOPT_CStatsPredUtils_H
#define GPOPT_CStatsPredUtils_H

#include "gpos/base.h"
#include "gpopt/base/CColRef.h"
#include "gpopt/operators/CExpression.h"

#include "naucrates/statistics/CStatsPred.h"

namespace gpopt
{

	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CStatsPredUtils
	//
	//	@doc:
	//		Utility functions for extracting statistics predicates
	//
	//---------------------------------------------------------------------------
	class CStatsPredUtils
	{

		private:

			// type of predicate expression
			enum EPredicateType
			{
				EptPoint, // filter with literals
				EptScIdent, // scalar ident that is convert into an equals true/false filter
				EptConj, // conjunctive filter
				EptDisj, // disjunctive filter
				EptLike, // LIKE filter
				EptNullTest, // null test, including IS NULL and IS NOT NULL
				EptUnsupported, // unsupported filter for statistics calculation

				EptSentinel
			};

			// shorthand for functions for extracting statistics filter from predicate expressions
			typedef CStatsPred * (PfPstatspred)(IMemoryPool *pmp, CExpression *pexprPred, CColRefSet *pcrsOuterRefs);

			// pair of predicate type and the corresponding function to extracts is statistics filter
			struct SScStatsfilterMapping
			{
				// type
				CStatsPredUtils::EPredicateType ept;

				// extractor function pointer
				PfPstatspred *pf;
			};

			// return the comparison type of an operator for the purpose of statistics computation
			static
			CStatsPred::EStatsCmpType Estatscmpt(IMDId *pmdid);

			// return the comparison type of an operator for the purpose of statistics computation
			static
			CStatsPred::EStatsCmpType Estatscmpt(const CWStringConst *pstrOpName);

			// extract statistics filtering information from boolean expression
			static
			CStatsPred *PstatspredBoolean(IMemoryPool *pmp, CExpression *pexprPred, CColRefSet *pcrsOuterRefs);

			// extract statistics filtering information from a scalar array compare operator
			static
			void ProcessArrayCmp(IMemoryPool *pmp, CExpression *pexprPred, DrgPstatspred *pdrgpstatspred);

			// create and add statistics filtering information for supported filters
			static
			void AddSupportedStatsFilters
				(
				IMemoryPool *pmp,
				DrgPstatspred *pdrgpstatspred,
				CExpression *pexprPred,
				CColRefSet *pcrsOuterRefs
				);

			// create a conjunctive statistics filter composed of the extracted components of the conjunction
			static
			CStatsPred *PstatspredConj(IMemoryPool *pmp, CExpression *pexprScalar, CColRefSet *pcrsOuterRefs);

			// create a disjunction statistics filter composed of the extracted components of the disjunction
			static
			CStatsPred *PstatspredDisj(IMemoryPool *pmp, CExpression *pexprPred, CColRefSet *pcrsOuterRefs);

			// return statistics filter type for the given expression
			static
			CStatsPredUtils::EPredicateType Ept(IMemoryPool *pmp, CExpression *pexprPred);

			// is the condition a conjunctive predicate
			static
			BOOL FConjunction(IMemoryPool *pmp, CExpression *pexprPred);

			// is the condition a boolean predicate
			static
			BOOL FBooleanScIdent(CExpression *pexprPred);

			// is the condition a point predicate
			static
			BOOL FPointPredicate(CExpression *pexprPred);

			// is the condition an IDF point predicate
			static
			BOOL FPointIDF(CExpression *pexprPred);

			// is the condition an INDF point predicate
			static
			BOOL FPointINDF(CExpression *pexprPred);

			// is the condition 'is null' on a scalar ident
			static
			BOOL FScalarIdentIsNull(CExpression *pexprPred);

			// is the condition 'is not null' on a scalar ident
			static
			BOOL FScalarIdentIsNotNull(CExpression *pexprPred);

			// extract statistics filtering information from a point comparison
			static
			CStatsPred *PstatspredPoint(IMemoryPool *pmp, CExpression *pexprPred, CColRefSet *pcrsOuterRefs);

			// extract statistics filtering information from a LIKE comparison
			static
			CStatsPred *PstatspredLike(IMemoryPool *pmp, CExpression *pexprPred, CColRefSet *pcrsOuterRefs);

			// extract statistics filtering information from a null test
			static
			CStatsPred *PstatspredNullTest(IMemoryPool *pmp, CExpression *pexprPred, CColRefSet *pcrsOuterRefs);

			// create an unsupported statistics predicate
			static
			CStatsPred *PstatspredUnsupported(IMemoryPool *pmp, CExpression *pexprPred, CColRefSet *pcrsOuterRefs);

			// generate a point predicate for expressions of the form colid CMP constant for which we support stats calculation;
			// else return an unsupported stats predicate
			static
			CStatsPred *Pstatspred(IMemoryPool *pmp, CExpression *pexpr);

			// return the statistics predicate comparison type based on the md identifier
			static
			CStatsPred::EStatsCmpType Estatscmptype(IMDId *pmdid);

			// helper function to extract statistics join filter from a given join predicate
			static
			CStatsPredJoin *PstatsjoinExtract
								(
								IMemoryPool *pmp,
								CExpression *pexprJoinPred,
								DrgPcrs *pdrgpcrsOutput, // array of output columns of join's relational inputs
								CColRefSet *pcrsOuterRefs,
								DrgPexpr *pdrgpexprUnsupported
								);

			// is the expression a comparison of scalar idents (or casted scalar idents).
			// If so, extract relevant info.
			static
			BOOL FCmpColsIgnoreCast
				(
				CExpression *pexpr,
				const CColRef **pcr1,
				CStatsPred::EStatsCmpType *pescmpt,
				const CColRef **pcr2
				);

		public:

			// extract statistics filter from scalar expression
			static
			CStatsPred *PstatspredExtract(IMemoryPool *pmp, CExpression *pexprScalar, CColRefSet *pcrsOuterRefs);

			// helper function to extract array of statistics join filter from an array of join predicates
			static
			DrgPstatspredjoin *PdrgpstatspredjoinExtract
								(
								IMemoryPool *pmp,
								CExpression *pexprScalar,
								DrgPcrs *pdrgpcrsOutput, // array of output columns of join's relational inputs
								CColRefSet *pcrsOuterRefs,
								CStatsPred **ppstatspredUnsupported
								);

			// helper function to extract array of statistics join filter from an expression handle
			static
			DrgPstatspredjoin *Pdrgpstatspredjoin(IMemoryPool *pmp, CExpressionHandle &exprhdl);

			// helper function to extract array of statistics join filter from an expression
			static
			DrgPstatspredjoin *Pdrgpstatspredjoin
								(
								IMemoryPool *pmp,
								CExpressionHandle &exprhdl,
								CExpression *pexprScalar,
								DrgPcrs *pdrgpcrsOutput,
								CColRefSet *pcrsOuterRefs
								);

			// is the predicate a conjunctive or disjunctive predicate
			static
			BOOL FConjOrDisjPred(CStatsPred *pstatspred);

			// is unsupported predicate on unknown column
			static
			BOOL FUnsupportedPredOnDefinedCol(CStatsPred *pstatspred);

	}; // class CStatsPredUtils
}


#endif // !GPOPT_CStatsPredUtils_H

// EOF
