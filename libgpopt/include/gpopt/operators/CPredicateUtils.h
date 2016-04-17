//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright 2011 EMC Corp.
//
//	@filename:
//		CPredicateUtils.h
//
//	@doc:
//		Utility functions for normalizing scalar predicates
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CPredicateUtils_H
#define GPOPT_CPredicateUtils_H

#include "gpos/base.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CScalarBoolOp.h"
#include "gpopt/mdcache/CMDAccessor.h"

namespace gpopt
{

	using namespace gpos;

	// fwd decl
	class CConstraintInterval;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CPredicateUtils
	//
	//	@doc:
	//		Utility functions for normalizing scalar predicates
	//
	//---------------------------------------------------------------------------
	class CPredicateUtils
	{

		private:

			// collect conjuncts recursively
			static
			void CollectConjuncts(CExpression *pexpr, DrgPexpr *pdrgpexpr);

			// collect disjuncts recursively
			static
			void CollectDisjuncts(CExpression *pexpr, DrgPexpr *pdrgpexpr);

			// check if a conjunct/disjunct can be skipped
			static
			BOOL FSkippable(CExpression *pexpr, BOOL fConjunction);

			// check if a conjunction/disjunction can be reduced to a constant True/False
			static
			BOOL FReducible(CExpression *pexpr, BOOL fConjunction);

			// check if given expression is a comparison over the given column
			static
			BOOL FComparison(CExpression *pexpr, CColRef *pcr, CColRefSet *pcrsAllowedRefs);
			
			// check whether the given expression contains references to only the given
			// columns. If pcrsAllowedRefs is NULL, then check whether the expression has
			// no column references and no volatile functions
			static
			BOOL FValidRefsOnly(CExpression *pexprScalar, CColRefSet *pcrsAllowedRefs);

			// reverse the comparison, for example "<" => ">", "<=" => "=> 
			static
			IMDType::ECmpType EcmptReverse(IMDType::ECmpType ecmpt);
			
			// helper to add explicit casting to left child of given equality predicate
			static
			CExpression *PexprAddCast(IMemoryPool *pmp, CExpression *pexprPred);

			// determine which predicates we should test implication for
			static
			BOOL FCheckPredicateImplication(CExpression *pexprPred);

			// check if predicate is implied by given equivalence classes
			static
			BOOL FImpliedPredicate(CExpression *pexprPred, DrgPcrs *pdrgpcrsEquivClasses);

			// helper to create index lookup comparison predicate with index key on left side
			static
			CExpression *PexprIndexLookupKeyOnLeft
				(
				IMemoryPool *pmp, 
				CMDAccessor *pmda,
				CExpression *pexprScalar,
				const IMDIndex *pmdindex,
				DrgPcr *pdrgpcrIndex, 
				CColRefSet *pcrsOuterRefs
				);

			// helper to create index lookup comparison predicate with index key on right side
			static
			CExpression *PexprIndexLookupKeyOnRight
				(
				IMemoryPool *pmp, 
				CMDAccessor *pmda,
				CExpression *pexprScalar,
				const IMDIndex *pmdindex,
				DrgPcr *pdrgpcrIndex, 
				CColRefSet *pcrsOuterRefs
				);

			// for all columns that appear in the given expression and are members
			// of the given set, replace these columns with NULL constants
			static
			CExpression *PexprReplaceColsWithNulls
				(
				IMemoryPool *pmp,
				CExpression *pexprScalar,
				CColRefSet *pcrs
				);

			// private ctor
			CPredicateUtils();

			// private dtor
			virtual
			~CPredicateUtils();

			// private copy ctor
			CPredicateUtils(const CPredicateUtils &);

		public:

			// check if the expression is a boolean scalar identifier
			static
			BOOL FBooleanScalarIdent(CExpression *pexprPred);

			// check if the expression is a negated boolean scalar identifier
			static
			BOOL FNegatedBooleanScalarIdent(CExpression *pexprPred);

			// is the given expression an equality comparison
			static
			BOOL FEquality(CExpression *pexpr);

			// is the given expression a scalar comparison
			static
			BOOL FComparison(CExpression *pexpr);
			
			// is the given expression a conjunction of equality comparisons
			static
			BOOL FConjunctionOfEqComparisons(IMemoryPool *pmp, CExpression *pexpr);
			
			// is the given expression a comparison of the given type
			static
			BOOL FComparison(CExpression *pexpr, IMDType::ECmpType ecmpt);

			// is the given array of expressions contain a volatile function like random()
			static
			BOOL FContainsVolatileFunction(DrgPexpr *pdrgpexpr);
			
			// is the given expressions contain a volatile function like random()
			static
			BOOL FContainsVolatileFunction(CExpression *pexpr);

			// is the given expression an equality comparison over two scalar identifiers
			static
			BOOL FPlainEquality(CExpression *pexpr);

			// is the given expression a self comparison on some column
			static
			BOOL FSelfComparison(CExpression *pexpr, IMDType::ECmpType *pecmpt);

			// eliminate self comparison if possible
			static
			CExpression *PexprEliminateSelfComparison(IMemoryPool *pmp, CExpression *pexpr);

			// is the given expression in the form (col1 Is NOT DISTINCT FROM col2)
			static
			BOOL FINDFScalarIdents(CExpression *pexpr);

			// is the given expression in the form (col1 IS DISTINCT FROM col2)
			static
			BOOL FIDFScalarIdents(CExpression *pexpr);

			// check if expression is an Is DISTINCT FROM FALSE expression
			static
			BOOL FIDFFalse(CExpression *pexpr);
 
			// is the given expression in the form (expr IS DISTINCT FROM expr)
			static
			BOOL FIDF(CExpression *pexpr);

			// is the given expression in the form (expr IS NOT DISTINCT FROM expr)
			static
			BOOL FINDF(CExpression *pexpr);

			// generate a conjunction of INDF expressions between corresponding columns in the given arrays
			static
			CExpression *PexprINDFConjunction(IMemoryPool *pmp, DrgPcr *pdrgpcrFirst, DrgPcr *pdrgpcrSecond);

			// is the given expression of the form (col IS DISTINCT FROM constant)
			// either the constant or the column can be casted
			static
			BOOL FIdentIDFConstIgnoreCast(CExpression *pexpr);

			// is the given expression of the form NOT (col IS DISTINCT FROM constant)
			// either the constant or the column can be casted
			static
			BOOL FIdentINDFConstIgnoreCast(CExpression *pexpr);

			// is the given expression a comparison between a scalar ident and a constant
			static
			BOOL FCompareIdentToConst(CExpression *pexpr);

			// is the given expression of the form (col comparison constant).
			// either the constant or the column can be casted
			static
			BOOL FIdentCmpConstIgnoreCast(CExpression *pexpr);

			// checks if comparison is between two columns, or a column and a const
			static
			BOOL FCompareColToConstOrCol(CExpression *pexprScalar);

			// is the given expression a comparison between a scalar ident and a constant array
			static
			BOOL FCompareIdentToConstArray(CExpression *pexpr);

			// is the given expression a scalar range or equality comparison
			static
			BOOL FRangeOrEqComp(CExpression *pexpr);

			// is the given expression an AND
			static
			BOOL FAnd
				(
				CExpression *pexpr
				)
			{
				return CUtils::FScalarBoolOp(pexpr, CScalarBoolOp::EboolopAnd);
			}

			// is the given expression an OR
			static
			BOOL FOr
				(
				CExpression *pexpr
				)
			{
				return CUtils::FScalarBoolOp(pexpr, CScalarBoolOp::EboolopOr);
			}

			// does the given expression have any NOT children?
			// is the given expression a NOT
			static
			BOOL FNot
				(
				CExpression *pexpr
				)
			{
				return CUtils::FScalarBoolOp(pexpr, CScalarBoolOp::EboolopNot);
			}

			// returns true iff the given expression is a Not operator whose child is a
			// scalar identifier
			static
			BOOL FNotIdent(CExpression *pexpr);

			// does the given expression have any NOT children?
			static
			BOOL FHasNegatedChild(CExpression *pexpr);

			// is the given expression an inner join
			static
			BOOL FInnerJoin
				(
				CExpression *pexpr
				)
			{
				return COperator::EopLogicalInnerJoin == pexpr->Pop()->Eopid() ||
						COperator::EopLogicalNAryJoin == pexpr->Pop()->Eopid();
			}

			//	append logical and scalar children of the given expression to
			static
			void CollectChildren(CExpression *pexpr, DrgPexpr *pdrgpexprLogical, DrgPexpr *pdrgpexprScalar);

			// is the given expression either a union or union all operator
			static
			BOOL FUnionOrUnionAll
				 (
				 CExpression *pexpr
				 )
			{
				return COperator::EopLogicalUnion == pexpr->Pop()->Eopid() ||
						COperator::EopLogicalUnionAll == pexpr->Pop()->Eopid();
			}

			// check if we can collapse the nth child of the given union / union all operator
			static
			BOOL FCollapsibleChildUnionUnionAll(CExpression *pexprParent, ULONG ulChildIndex);

			// if the nth child of the given union/union all expression is also an union / union all expression,
			// then collect the later's children and set the input columns of the new n-ary union/unionall operator
			static
			void CollectGrandChildrenUnionUnionAll
					(
					IMemoryPool *pmp,
					CExpression *pexpr,
					ULONG ulChildIndex,
					DrgPexpr *pdrgpexprResult,
					DrgDrgPcr *pdrgdrgpcrResult
					);

			// extract conjuncts from a scalar tree
			static
			DrgPexpr *PdrgpexprConjuncts(IMemoryPool *pmp, CExpression *pexpr);

			// extract disjuncts from a scalar tree
			static
			DrgPexpr *PdrgpexprDisjuncts(IMemoryPool *pmp, CExpression *pexpr);

			// extract equality predicates on scalar identifier from a list of scalar expressions
			static
			DrgPexpr *PdrgpexprPlainEqualities(IMemoryPool *pmp, DrgPexpr *pdrgpexpr);
			
			// create conjunction/disjunction
			static
			CExpression *PexprConjDisj(IMemoryPool *pmp, DrgPexpr *Pdrgpexpr, BOOL fConjunction);
			
			// create conjunction/disjunction of two expressions
			static
			CExpression *PexprConjDisj(IMemoryPool *pmp, CExpression *pexprOne, CExpression *pexprTwo, BOOL fConjunction);
			
			// create conjunction
			static
			CExpression *PexprConjunction(IMemoryPool *pmp, DrgPexpr *pdrgpexpr);

			// create conjunction of two expressions
			static
			CExpression *PexprConjunction(IMemoryPool *pmp, CExpression *pexprOne, CExpression *pexprTwo);

			// create disjunction of two expressions
			static
			CExpression *PexprDisjunction(IMemoryPool *pmp, CExpression *pexprOne, CExpression *pexprTwo);
 
			// expand disjuncts in the given array by converting ArrayComparison to AND/OR tree and deduplicating resulting disjuncts
			static
			DrgPexpr *PdrgpexprExpandDisjuncts(IMemoryPool *pmp, DrgPexpr *pdrgpexprDisjuncts);

			// expand conjuncts in the given array by converting ArrayComparison to AND/OR tree and deduplicating resulting conjuncts
			static
			DrgPexpr *PdrgpexprExpandConjuncts(IMemoryPool *pmp, DrgPexpr *pdrgpexprConjuncts);

			// check if the given expression is a boolean expression on the
			// given column, e.g. if its of the form "ScalarIdent(pcr)" or 
			// "Not(ScalarIdent(pcr))"
			static
			BOOL FBoolPredicateOnColumn(CExpression *pexpr, CColRef *pcr);
			
			// check if the given expression is a scalar array cmp expression on the
			// given column
			static
			BOOL FScArrayCmpOnColumn(CExpression *pexpr, CColRef *pcr, BOOL fConstOnly);
			
			// check if the given expression is a null check on the given column
			// i.e. "is null" or "is not null"
			static
			BOOL FNullCheckOnColumn(CExpression *pexpr, CColRef *pcr);

			// check if the given expression is a disjunction of scalar cmp 
			// expressions on the given column
			static
			BOOL FDisjunctionOnColumn(IMemoryPool *pmp, CExpression *pexpr, CColRef *pcr, CColRefSet *pcrsAllowedRefs);
			
			// check if the given comparison type is one of the range comparisons, i.e. 
			// LT, GT, LEq, GEq, Eq
			static 
			BOOL FRangeComparison(IMDType::ECmpType ecmpt);
			
			// create disjunction
			static
			CExpression *PexprDisjunction(IMemoryPool *pmp, DrgPexpr *Pdrgpexpr);
			
			// find a predicate that can be used for partition pruning with the given part key 
			static
			CExpression *PexprPartPruningPredicate
				(
				IMemoryPool *pmp,
				const DrgPexpr *pdrgpexpr, 
				CColRef *pcrPartKey,
				CExpression *pexprCol,
				CColRefSet *pcrsAllowedRefs
				);

			// append the conjuncts from the given expression to the given array, removing
			// any duplicates, and return the resulting array
			static
			DrgPexpr *PdrgpexprAppendConjunctsDedup
				(
				IMemoryPool *pmp,
				DrgPexpr *pdrgpexpr,
				CExpression *pexpr
				);
			
			// extract interesting conditions involving the partitioning keys
			static
			CExpression *PexprExtractPredicatesOnPartKeys
				(
				IMemoryPool *pmp,
				CExpression *pexprScalar,
				DrgDrgPcr *pdrgpdrgpcrPartKeys,
				CColRefSet *pcrsAllowedRefs,
				BOOL fUseConstraints
				);
			
			// extract the constraint on the given column and return the corresponding
			// scalar expression
			static
			CExpression *PexprPredicateCol
				(
				IMemoryPool *pmp,
				CConstraint *pcnstr,
				CColRef *pcr,
				BOOL fUseConstraints
				);

			// extract components of a comparison expression on the given key
			static 
			void ExtractComponents
					(
					CExpression *pexprScCmp, 
					CColRef *pcrKey, 
					CExpression **ppexprKey, 
					CExpression **ppexprOther,
					IMDType::ECmpType *pecmpt
					);

			// check if given column is a constant
			static
			BOOL FConstColumn(CConstraint *pcnstr, const CColRef *pcr);
			
			// check if given column is a disjunction of constants
			static
			BOOL FColumnDisjunctionOfConst(CConstraint *pcnstr, const CColRef *pcr);
			
			// check if given column is a disjunction of constants
			static
			BOOL FColumnDisjunctionOfConst(CConstraintInterval *pcnstr, const CColRef *pcr);
			
			// split predicates into those that refer to an index key, and those that don't 
			static
			void ExtractIndexPredicates
				(
				IMemoryPool *pmp,
				CMDAccessor *pmda,
				DrgPexpr *pdrgpexprPredicate,
				const IMDIndex *pmdindex,
				DrgPcr *pdrgpcrIndex,
				DrgPexpr *pdrgpexprIndex,
				DrgPexpr *pdrgpexprResidual,
				CColRefSet *pcrsAcceptedOuterRefs = NULL // outer refs that are acceptable in an index predicate
				);

			// return the inverse of given comparison expression
			static
			CExpression *PexprInverseComparison(IMemoryPool *pmp, CExpression *pexprCmp);

			// add explicit casting to equality operations between compatible types
			static
			DrgPexpr *PdrgpexprCastEquality(IMemoryPool *pmp, CExpression *pexpr);

			// prune unnecessary equality operations
			static
			CExpression *PexprPruneSuperfluosEquality(IMemoryPool *pmp, CExpression *pexpr);

			// remove conjuncts that are implied based on child columns equivalence classes
			static
			CExpression *PexprRemoveImpliedConjuncts(IMemoryPool *pmp, CExpression *pexprScalar, CExpressionHandle &exprhdl);

			//	check if given correlations are valid for semi join operator;
			static
			BOOL FValidSemiJoinCorrelations
				(
				IMemoryPool *pmp,
				CExpression *pexprOuter,
				CExpression *pexprInner,
				DrgPexpr *pdrgpexprCorrelations
				);

			// check if given expression is composed of simple column equalities that use columns from given column set
			static
			BOOL FSimpleEqualityUsingCols
				(
				IMemoryPool *pmp,
				CExpression *pexprScalar,
				CColRefSet *pcrs
				);

			// check if given expression is a valid index lookup predicate
			// return (modified) predicate suited for index lookup
			static
			CExpression *PexprIndexLookup
				(
				IMemoryPool *pmp,
				CMDAccessor *pmda,
				CExpression *pexpPred, 
				const IMDIndex *pmdindex,
				DrgPcr *pdrgpcrIndex, 
				CColRefSet *pcrsOuterRefs
				);

			// split given scalar expression into two conjunctions; without and with outer references
			static
			void SeparateOuterRefs(IMemoryPool *pmp, CExpression *pexprScalar, CColRefSet *pcrsOuter, CExpression **ppexprLocal, CExpression **ppexprOuterRef);

			// add explicit casting on the input expression to the destination type
			static
			CExpression *PexprCast(IMemoryPool *pmp, CMDAccessor *pmda, CExpression *pexpr, IMDId *pmdidDest);

			// is the condition a LIKE predicate
			static
			BOOL FLikePredicate(IMDId *pmdid);

			// is the condition a LIKE predicate
			static
			BOOL FLikePredicate(CExpression *pexprPred);

			// extract the components of a LIKE predicate
			static
			void ExtractLikePredComponents(CExpression *pexprPred, CExpression **ppexprScIdent, CExpression **ppexprConst);

			// check if scalar expression is null-rejecting and uses columns from given column set
			static
			BOOL FNullRejecting(IMemoryPool *pmp, CExpression *pexprScalar, CColRefSet *pcrs);

			// returns true iff the given predicate pexprPred is compatible with the given index pmdindex
			static
			BOOL FCompatibleIndexPredicate
				(
				CExpression *pexprPred,
				const IMDIndex *pmdindex,
				DrgPcr *pdrgpcrIndex,
				CMDAccessor *pmda
				);

			// returns true iff all predicates in the given array are compatible with the given index
			static
			BOOL FCompatiblePredicates
				(
				DrgPexpr *pdrgpexprPred,
				const IMDIndex *pmdindex,
				DrgPcr *pdrgpcrIndex,
				CMDAccessor *pmda
				);

			// check if the expensive CNF conversion is beneficial in finding predicate for hash join
			static
			BOOL FConvertToCNF(CExpression *pexpr, 	CExpression *pexprOuter, CExpression *pexprInner);

	}; // class CPredicateUtils
}


#endif // !GPOPT_CPredicateUtils_H

// EOF
