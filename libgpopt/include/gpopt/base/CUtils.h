//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CUtils.h
//
//	@doc:
//		Optimizer utility functions
//---------------------------------------------------------------------------
#ifndef GPOPT_CUtils_H
#define GPOPT_CUtils_H

#include "gpos/error/CAutoTrace.h"
#include "gpos/common/CHashSet.h"

#include "gpopt/base/CColRef.h"
#include "gpopt/base/COrderSpec.h"
#include "gpopt/base/CWindowFrame.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CScalarArrayCmp.h"
#include "gpopt/operators/CScalarCmp.h"
#include "gpopt/operators/CScalarConst.h"
#include "gpopt/operators/CScalarBoolOp.h"
#include "gpopt/operators/CScalarProjectList.h"
#include "gpopt/operators/CScalarSubquery.h"
#include "gpopt/operators/CScalarAggFunc.h"
#include "naucrates/md/CMDTypeInt4GPDB.h"
#include "naucrates/statistics/IStatistics.h"

// fwd declarations
namespace gpmd
{
	class IMDId;
}

namespace gpopt
{
	using namespace gpos;
	
	// fwd declaration
	class CMemo;
	class CLogicalCTEConsumer;
	class CLogicalCTEProducer;
	class CDistributionSpec;
	class IConstExprEvaluator;
	class CLogical;
	class CLogicalGbAgg;

	//---------------------------------------------------------------------------
	//	@class:
	//		CUtils
	//
	//	@doc:
	//		General utility functions
	//
	//---------------------------------------------------------------------------
	class CUtils
	{

		private:

			// check if the expression is a scalar boolean const
			static
			BOOL FScalarConstBool(CExpression *pexpr, BOOL fVal);

			// check if two expressions have the same children in any order
			static
			BOOL FMatchChildrenUnordered(const CExpression *pexprLeft, const CExpression *pexprRight);

			// check if two expressions have the same children in the same order
			static
			BOOL FMatchChildrenOrdered(const CExpression *pexprLeft, const CExpression *pexprRight);

			// checks that the given type has all the comparisons: Eq, NEq, L, LEq, G, GEq
			static
			BOOL FHasAllDefaultComparisons(const IMDType *pmdtype);

			//	append the expressions in the source array to destination array
			static
			void AppendArrayExpr(DrgPexpr *pdrgpexprSrc, DrgPexpr *pdrgpexprDest);

		public:

			enum EExecLocalityType
			{
				EeltMaster,
				EeltSegments,
				EeltSingleton
			};

#ifdef GPOS_DEBUG

			// print given expression to debug trace
			static
			void PrintExpression(CExpression *pexpr);
			
			// print memo to debug trace
			static
			void PrintMemo(CMemo *pmemo);

			static
			IOstream &OsPrintDrgPcoldesc(IOstream &os, DrgPcoldesc *pdrgpcoldescIncludedCols, ULONG ulLength);
#endif // GPOS_DEBUG

			//-------------------------------------------------------------------
			// Helpers for generating expressions
			//-------------------------------------------------------------------

			// recursively check for a plan with CTE, if both CTEProducer and CTEConsumer are executed on the same locality.
			// raises an exception if CTE Producer and CTE Consumer does not have the same locality
			static
			void ValidateCTEProducerConsumerLocality(IMemoryPool *pmp, CExpression *pexpr, EExecLocalityType edt, HMUlUl *phmulul);

			// check is a comparison between given types or a comparison after casting
			// one side to an another exists
			static
			BOOL FCmpOrCastedCmpExists(IMDId *pmdidLeft, IMDId *pmdidRight, IMDType::ECmpType ecmpt);

			// return the mdid of the given scalar comparison between the two types
			static
			IMDId *PmdidScCmp(IMemoryPool *pmp, CMDAccessor *pmda, IMDId *pmdidLeft, IMDId *pmdidRight, IMDType::ECmpType ecmpt);
			
			// generate a comparison expression for two column references
			static
			CExpression *PexprScalarCmp(IMemoryPool *pmp, const CColRef *pcrLeft, const CColRef *pcrRight, CWStringConst strOp, IMDId *pmdidOp);
	
			// generate a comparison expression for a column reference and an expression
			static
			CExpression *PexprScalarCmp(IMemoryPool *pmp, const CColRef *pcrLeft, CExpression *pexprRight, CWStringConst strOp, IMDId *pmdidOp);

			// generate a comparison expression for an expression and a column reference
			static
			CExpression *PexprScalarCmp(IMemoryPool *pmp, CExpression *pexprLeft, const CColRef *pcrRight, CWStringConst strOp, IMDId *pmdidOp);

			// generate a comparison expression for two expressions
			static
			CExpression *PexprScalarCmp(IMemoryPool *pmp, CExpression *pexprLeft, CExpression *pexprRight, CWStringConst strOp, IMDId *pmdidOp);

			// generate a comparison expression for a column reference and an expression
			static
			CExpression *PexprScalarCmp(IMemoryPool *pmp, const CColRef *pcrLeft, CExpression *pexprRight, IMDType::ECmpType ecmpt);

			// generate a comparison expression between two column references
			static
			CExpression *PexprScalarCmp(IMemoryPool *pmp, const CColRef *pcrLeft, const CColRef *pcrRight, IMDType::ECmpType ecmpt);

			// generate a comparison expression between an expression and a column reference
			static
			CExpression *PexprScalarCmp(IMemoryPool *pmp, CExpression *pexprLeft, const CColRef *pcrRight, IMDType::ECmpType ecmpt);

			// generate a comparison expression for two expressions
			static
			CExpression *PexprScalarCmp(IMemoryPool *pmp, CExpression *pexprLeft, CExpression *pexprRight, IMDType::ECmpType ecmpt);

			// generate a comparison against Zero
			static
			CExpression *PexprCmpWithZero(IMemoryPool *pmp, CExpression *pexprLeft, IMDId *pmdidTypeLeft, IMDType::ECmpType ecmptype);

			// generate an equality comparison expression for column references
			static
			CExpression *PexprScalarEqCmp(IMemoryPool *pmp, const CColRef *pcrLeft, const CColRef *pcrRight);
			
			// generate an equality comparison expression for two expressions
			static
			CExpression *PexprScalarEqCmp(IMemoryPool *pmp, CExpression *pexprLeft, CExpression *pexprRight);
						
			// generate an equality comparison expression for a column reference and an expression
			static
			CExpression *PexprScalarEqCmp(IMemoryPool *pmp, const CColRef *pcrLeft, CExpression *pexprRight);

			// generate an equality comparison expression for an expression and a column reference
			static
			CExpression *PexprScalarEqCmp(IMemoryPool *pmp, CExpression *pexprLeft, const CColRef *pcrRight);

			// generate an array comparison expression for a column reference and an expression
			static
			CExpression *PexprScalarArrayCmp(IMemoryPool *pmp, CScalarArrayCmp::EArrCmpType earrcmptype, IMDType::ECmpType ecmptype, DrgPexpr *pexprScalarChildren, const CColRef *pcr);

			// generate an Is Distinct From expression
			static
			CExpression *PexprIDF(IMemoryPool *pmp, CExpression *pexprLeft, CExpression *pexprRight);

			// generate an Is NOT Distinct From expression for two column references
			static
			CExpression *PexprINDF(IMemoryPool *pmp, const CColRef *pcrLeft, const CColRef *pcrRight);

			// generate an Is NOT Distinct From expression for scalar expressions
			static
			CExpression *PexprINDF(IMemoryPool *pmp, CExpression *pexprLeft, CExpression *pexprRight);

			// generate an Is NULL expression
			static
			CExpression *PexprIsNull(IMemoryPool *pmp, CExpression *pexpr);

			// generate an Is NOT NULL expression
			static
			CExpression *PexprIsNotNull(IMemoryPool *pmp, CExpression *pexpr);

			// generate an Is NOT FALSE expression
			static
			CExpression *PexprIsNotFalse(IMemoryPool *pmp, CExpression *pexpr);

			// find if a scalar expression uses a nullable columns from the output of a logical expression
			static
			BOOL FUsesNullableCol(IMemoryPool *pmp, CExpression *pexprScalar, CExpression *pexprLogical);

			// generate a scalar op expression for a column reference and an expression
			static
			CExpression *PexprScalarOp(IMemoryPool *pmp, const CColRef *pcrLeft, CExpression *pexpr, CWStringConst strOp, IMDId *pmdidOp, IMDId *pmdidReturnType = NULL);

			// generate a scalar bool op expression
			static
			CExpression *PexprScalarBoolOp(IMemoryPool *pmp, CScalarBoolOp::EBoolOperator eboolop, DrgPexpr *pdrgpexpr);

			// negate the given expression
			static
			CExpression *PexprNegate(IMemoryPool *pmp, CExpression *pexpr);

			// generate a scalar ident expression
			static
			CExpression *PexprScalarIdent(IMemoryPool *pmp, const CColRef *pcr);

			// generate a scalar project element expression
			static
			CExpression *PexprScalarProjectElement(IMemoryPool *pmp, CColRef *pcr, CExpression *pexpr);

			// generate an aggregate function operator
			static
			CScalarAggFunc *PopAggFunc
				(
				IMemoryPool *pmp,
				IMDId *pmdidAggFunc,
				const CWStringConst *pstrAggFunc,
				BOOL fDistinct,
				EAggfuncStage eaggfuncstage,
				BOOL fSplit,
				IMDId *pmdidResolvedReturnType = NULL // return type to be used if original return type is ambiguous
				);

			// generate an aggregate function
			static
			CExpression *PexprAggFunc
				(
				IMemoryPool *pmp,
				IMDId *pmdidAggFunc,
				const CWStringConst *pstrAggFunc,
				const CColRef *pcr,
				BOOL fDistinct,
				EAggfuncStage eaggfuncstage,
				BOOL fSplit
				);

			// generate a count(*) expression
			static
			CExpression *PexprCountStar(IMemoryPool *pmp);

			// generate a GbAgg with count(*) function over the given expression
			static
			CExpression *PexprCountStar(IMemoryPool *pmp, CExpression *pexprLogical);

			// return True if passed expression is a Project Element defined on count(*)/count(any) agg
			static
			BOOL FCountAggProjElem(CExpression *pexprPrjElem, CColRef **ppcrCount);

			// check if given expression has a count(*)/count(Any) agg
			static
			BOOL FHasCountAgg(CExpression *pexpr, CColRef **ppcrCount);

			// check if given expression has count matching the given column, returns the Logical GroupBy Agg above
			static
			BOOL FHasCountAggMatchingColumn(const CExpression *pexpr, const CColRef *pcr, const CLogicalGbAgg** ppgbAgg);

			// generate a GbAgg with count(*) and sum(col) over the given expression
			static
			CExpression *PexprCountStarAndSum(IMemoryPool *pmp, const CColRef *pcr, CExpression *pexprLogical);

			// generate a sum(col) expression
			static
			CExpression *PexprSum(IMemoryPool *pmp, const CColRef *pcr);

			// generate a GbAgg with sum(col) expressions for all columns in the given array
			static
			CExpression *PexprGbAggSum(IMemoryPool *pmp, CExpression *pexprLogical, DrgPcr *pdrgpcrSum);

			// generate a count(col) expression
			static
			CExpression *PexprCount(IMemoryPool *pmp, const CColRef *pcr, BOOL fDistinct);

			// generate a min(col) expression
			static
			CExpression *PexprMin(IMemoryPool *pmp, CMDAccessor *pmda, const CColRef *pcr);

			// generate an aggregate expression
			static
			CExpression *PexprAgg(IMemoryPool *pmp, CMDAccessor *pmda, IMDType::EAggType eagg, const CColRef *pcr, BOOL fDistinct);

			// generate a select expression
			static
			CExpression *PexprLogicalSelect(IMemoryPool *pmp, CExpression *pexpr, CExpression *pexprPredicate);

			// if predicate is True return logical expression, otherwise return a new select node
			static
			CExpression *PexprSafeSelect(IMemoryPool *pmp, CExpression *pexprLogical, CExpression *pexprPredicate);

			// generate a select expression, if child is another Select expression collapse both Selects into one expression
			static
			CExpression *PexprCollapseSelect(IMemoryPool *pmp, CExpression *pexpr, CExpression *pexprPredicate);

			// generate a project expression
			static
			CExpression *PexprLogicalProject(IMemoryPool *pmp, CExpression *pexpr, CExpression *pexprPrjList, BOOL fNewComputedCol);

			// generate a sequence project expression
			static
			CExpression *PexprLogicalSequenceProject(IMemoryPool *pmp, CDistributionSpec *pds, DrgPos *pdrgpos, DrgPwf *pdrgpwf, CExpression *pexpr, CExpression *pexprPrjList);

			// generate a projection of NULL constants
			// to the map 'phmulcr', and add the mappings to the phmulcr map if not NULL
			static
			CExpression *PexprLogicalProjectNulls
				(
				IMemoryPool *pmp,
				DrgPcr *pdrgpcr,
				CExpression *pexpr,
				HMUlCr *phmulcr = NULL
				);

			// construct a project list using the given columns and datums
			// store the mapping in the phmulcr map if not NULL
			static
			CExpression *PexprScalarProjListConst
				(
				IMemoryPool *pmp,
				DrgPcr *pdrgpcr,
				DrgPdatum *pdrgpdatum,
				HMUlCr *phmulcr
				);

			// generate a project expression
			static
			CExpression *PexprAddProjection(IMemoryPool *pmp, CExpression *pexpr, CExpression *pexprProjected);

			// generate a project expression with one or more additional project elements
			static
			CExpression *PexprAddProjection(IMemoryPool *pmp, CExpression *pexpr, DrgPexpr *pdrgpexprProjected);

			// generate an aggregate expression
			static
			CExpression *PexprLogicalGbAggGlobal
							(
							IMemoryPool *pmp,
							DrgPcr *pdrgpcr,
							CExpression *pexpr,
							CExpression *pexprPrL
							);

			// generate an aggregate expression
			static
			CExpression *PexprLogicalGbAgg
							(
							IMemoryPool *pmp,
							DrgPcr *pdrgpcr,
							CExpression *pexpr,
							CExpression *pexprPrL,
							COperator::EGbAggType egbaggtype
							);

			// check if the aggregate is local or global
			static
			BOOL FHasGlobalAggFunc(const CExpression *pexprProjList);

			// generate a bool expression
			static
			CExpression *PexprScalarConstBool(IMemoryPool *pmp, BOOL fVal, BOOL fNull = false);

			// generate an int4 expression
			static
			CExpression *PexprScalarConstInt4(IMemoryPool *pmp, INT iVal);

			// generate an int8 expression
			static
			CExpression *PexprScalarConstInt8(IMemoryPool *pmp, LINT lVal, BOOL fNull = false);
			
			// generate an oid constant expression
			static
			CExpression *PexprScalarConstOid(IMemoryPool *pmp, OID oidVal);

			// comparison operator type
			static
			IMDType::ECmpType Ecmpt(IMDId *pmdid);
						
			// comparison operator type
			static
			IMDType::ECmpType Ecmpt(CMDAccessor *pmda, IMDId *pmdid);
			
			// generate a binary join expression
			template<class T>
			static
			CExpression *PexprLogicalJoin(IMemoryPool *pmp, CExpression *pexprLeft, CExpression *pexprRight, CExpression *pexprPredicate);

			// generate an apply expression
			template<class T>
			static
			CExpression *PexprLogicalApply
				(
				IMemoryPool *pmp,
				CExpression *pexprLeft,
				CExpression *pexprRight,
				CExpression *pexprPred = NULL
				);

			// generate an apply expression with a known inner column
			template<class T>
			static
			CExpression *PexprLogicalApply
				(
				IMemoryPool *pmp,
				CExpression *pexprLeft,
				CExpression *pexprRight,
				const CColRef *pcrInner,
				COperator::EOperatorId eopidOriginSubq,
				CExpression *pexprPred = NULL
				);

			// generate an apply expression with a known array of inner columns
			template<class T>
			static
			CExpression *PexprLogicalApply
				(
				IMemoryPool *pmp,
				CExpression *pexprLeft,
				CExpression *pexprRight,
				DrgPcr *pdrgpcrInner,
				COperator::EOperatorId eopidOriginSubq,
				CExpression *pexprPred = NULL
				);

			// generate a correlated apply for quantified subquery with a known array of inner columns
			template<class T>
			static
			CExpression* PexprLogicalCorrelatedQuantifiedApply
				(
				IMemoryPool *pmp,
				CExpression *pexprLeft,
				CExpression *pexprRight,
				DrgPcr *pdrgpcrInner,
				COperator::EOperatorId eopidOriginSubq,
				CExpression *pexprPred = NULL
				);

			//-------------------------------------------------------------------
			// Helpers for partitioning
			//-------------------------------------------------------------------

			// extract the nth partition key from the given array of partition keys
			static
			CColRef *PcrExtractPartKey(DrgDrgPcr *pdrgpdrgpcr, ULONG ulLevel);

			//-------------------------------------------------------------------
			// Helpers for comparisons
			//-------------------------------------------------------------------

			// deduplicate array of expressions
			static
			DrgPexpr *PdrgpexprDedup(IMemoryPool *pmp, DrgPexpr *pdrgpexpr);

			// deep equality of expression trees
			static
			BOOL FEqual(const CExpression *pexprLeft, const CExpression *pexprRight);

			// compare expression against an array of expressions
			static
			BOOL FEqualAny(const CExpression *pexpr, const DrgPexpr *pdrgpexpr);

			// deep equality of expression arrays
			static
			BOOL FEqual(const DrgPexpr *pdrgpexprLeft, const DrgPexpr *pdrgpexprRight);

			// check if first expression array contains all expressions in second array
			static
			BOOL FContains(const DrgPexpr *pdrgpexprFst, const DrgPexpr *pdrgpexprSnd);

			// return the number of occurrences of the given expression in the given
			// array of expressions
			static
			ULONG UlOccurrences(const CExpression *pexpr, DrgPexpr *pdrgpexpr);

			//-------------------------------------------------------------------
			// Helpers for datums
			//-------------------------------------------------------------------

			// check to see if the expression is a scalar const TRUE
			static
			BOOL FScalarConstTrue(CExpression *pexpr);

			// check to see if the expression is a scalar const FALSE
			static
			BOOL FScalarConstFalse(CExpression *pexpr);

			// check if the given expression is an INT, the template parameter is an INT type
			template<class T>
			static
			BOOL FScalarConstInt(CExpression *pexpr);

			//-------------------------------------------------------------------
			// Helpers for printing
			//-------------------------------------------------------------------

			// column reference array print helper
			static IOstream &
			OsPrintDrgPcr(IOstream &os,
						  const DrgPcr *pdrgpcr,
						  ULONG = gpos::ulong_max);

			//-------------------------------------------------------------------
			// Helpers for column reference sets
			//-------------------------------------------------------------------

			// return an array of non-system columns in the given set
			static
			DrgPcr *PdrgpcrNonSystemCols(IMemoryPool *pmp, CColRefSet *pcrs);

			// create an array of output columns including a key for grouping
			static
			DrgPcr *PdrgpcrGroupingKey(IMemoryPool *pmp, CExpression *pexpr, DrgPcr **ppdrgpcrKey);

			// add an equivalence class (col ref set) to the array. If the new equiv
			// class contains columns from existing equiv classes, then these are merged
			static
			DrgPcrs *PdrgpcrsAddEquivClass(IMemoryPool *pmp, CColRefSet *pcrsNew, DrgPcrs *pdrgpcrs);

			// merge 2 arrays of equivalence classes
			static
			DrgPcrs *PdrgpcrsMergeEquivClasses
						(
						IMemoryPool *pmp,
						DrgPcrs *pdrgpcrsFst,
						DrgPcrs *pdrgpcrsSnd
						);

			// intersect 2 arrays of equivalence classes
			static
			DrgPcrs *PdrgpcrsIntersectEquivClasses
						(
						IMemoryPool *pmp,
						DrgPcrs *pdrgpcrsFst,
						DrgPcrs *pdrgpcrsSnd
						);

			// return a copy of equivalence classes from all children
			static
			DrgPcrs *PdrgpcrsCopyChildEquivClasses(IMemoryPool *pmp, CExpressionHandle &exprhdl);

			// return a copy of the given array of columns, excluding the columns
			// in the given colrefset
			static
			DrgPcr *PdrgpcrExcludeColumns(IMemoryPool *pmp, DrgPcr *pdrgpcrOriginal, CColRefSet *pcrsExcluded);

			//-------------------------------------------------------------------
			// General helpers
			//-------------------------------------------------------------------

			// append elements from input array to output array, starting from given index, after add-refing them
			template <class T, void (*pfnDestroy)(T*)>
			static
			void AddRefAppend(CDynamicPtrArray<T, pfnDestroy> *pdrgptOutput, CDynamicPtrArray<T, pfnDestroy> *pdrgptInput, ULONG ulStart = 0);

			// check for existence of subqueries
			static
			BOOL FHasSubquery(CExpression *pexpr);

			// check existence of subqueries or Apply operators in deep expression tree
			static
			BOOL FHasSubqueryOrApply(CExpression *pexpr, BOOL fCheckRoot = true);

			// check existence of correlated apply operators in deep expression tree
			static
			BOOL FHasCorrelatedApply(CExpression *pexpr, BOOL fCheckRoot = true);

			// check for existence of outer references
			static
			BOOL FHasOuterRefs(CExpression *pexpr);

			// check if a given operator is a logical join
			static
			BOOL FLogicalJoin(COperator *pop);

			// check if a given operator is a logical set operation
			static
			BOOL FLogicalSetOp(COperator *pop);

			// check if a given operator is a logical unary operator
			static
			BOOL FLogicalUnary(COperator *pop);

			// check if a given operator is a physical join
			static
			BOOL FPhysicalJoin(COperator *pop);

			// check if a given operator is a physical outer join
			static
			BOOL FPhysicalOuterJoin(COperator *pop);

			// check if a given operator is a physical scan
			static
			BOOL FPhysicalScan(COperator *pop);

			// check if a given operator is a physical agg
			static
			BOOL FPhysicalAgg(COperator *pop);

			// check if given expression has any one stage agg nodes
			static
			BOOL FHasOneStagePhysicalAgg(const CExpression *pexpr);

			// check if a given operator is a physical motion
			static
			BOOL FPhysicalMotion(COperator *pop);

			// check if duplicate values can be generated by the given Motion expression
			static
			BOOL FDuplicateHazardMotion(CExpression *pexprMotion);

			// check if a given operator is an enforcer
			static
			BOOL FEnforcer(COperator *pop);

			// check if a given operator is a hash join
			static
			BOOL FHashJoin(COperator *pop);

			// check if a given operator is a correlated nested loops join
			static
			BOOL FCorrelatedNLJoin(COperator *pop);

			// check if a given operator is a nested loops join
			static
			BOOL FNLJoin(COperator *pop);

			// check if a given operator is an Apply
			static
			BOOL FApply(COperator *pop);

			// check if a given operator is a correlated Apply
			static
			BOOL FCorrelatedApply(COperator *pop);

			// check if a given operator is left semi apply
			static
			BOOL FLeftSemiApply(COperator *pop);

			// check if a given operator is left anti semi apply
			static
			BOOL FLeftAntiSemiApply(COperator *pop);

			// check if a given operator is a subquery
			static
			BOOL FSubquery(COperator *pop);

			// check if a given operator is existential subquery
			static
			BOOL FExistentialSubquery(COperator *pop);

			// check if a given operator is quantified subquery
			static
			BOOL FQuantifiedSubquery(COperator *pop);

			// check if given expression is a Project Element with scalar subquery
			static
			BOOL FProjElemWithScalarSubq(CExpression *pexpr);

			// check if given expression is a scalar subquery with a ConstTableGet as the only child
			static
			BOOL FScalarSubqWithConstTblGet(CExpression *pexpr);

			// check if given expression is a Project on ConstTable with one scalar subquery in Project List
			static
			BOOL FProjectConstTableWithOneScalarSubq(CExpression *pexpr);

			// check if an expression is a 0 offset
			static
			BOOL FScalarConstIntZero(CExpression *pexprOffset);

			// check if a limit expression has 0 offset
			static
			BOOL FHasZeroOffset(CExpression *pexpr);
			
			// check if expression is scalar comparison
			static
			BOOL FScalarCmp(CExpression *pexpr);
			
			// check if expression is scalar array comparison
			static
			BOOL FScalarArrayCmp(CExpression *pexpr);

			// check if given operator exists in the given list
			static
			BOOL FOpExists(const COperator *pop, const COperator::EOperatorId *peopid, ULONG ulOps);

			// check if given expression has any operator in the given list
			static
			BOOL FHasOp(const CExpression *pexpr, const COperator::EOperatorId *peopid, ULONG ulOps);

			// return number of inlinable CTEs in the given expression
			static
			ULONG UlInlinableCTEs(CExpression *pexpr, ULONG ulDepth = 1);

			// return number of joins in the given expression
			static
			ULONG UlJoins(CExpression *pexpr);

			// return number of subqueries in the given expression
			static
			ULONG UlSubqueries(CExpression *pexpr);

			// check if expression is scalar bool op
			static
			BOOL FScalarBoolOp(CExpression *pexpr);

			// is the given expression a scalar bool op of the passed type?
			static
			BOOL FScalarBoolOp(CExpression *pexpr, CScalarBoolOp::EBoolOperator eboolop);

			// check if expression is scalar null test
			static
			BOOL FScalarNullTest(CExpression *pexpr);

			// check if given expression is a NOT NULL predicate
			static
			BOOL FScalarNotNull(CExpression *pexpr);

			// check if expression is scalar identifier
			static
			BOOL FScalarIdent(CExpression *pexpr);

			static
			BOOL FScalarConstAndScalarIdentArray(CExpression *pexprArray);

			// check if expression is scalar identifier of boolean type
			static
			BOOL FScalarIdentBoolType(CExpression *pexpr);

			// check if expression is scalar array
			static
			BOOL FScalarArray(CExpression *pexpr);

			// returns number of children or constants of it is all constants
			static
			ULONG UlScalarArrayArity(CExpression *pexpr);

			// returns constant operator of a scalar array expression
			static
			CScalarConst *PScalarArrayConstChildAt(CExpression *pexprArray, ULONG ul);

			// returns constant expression of a scalar array expression
			static
			CExpression *PScalarArrayExprChildAt(IMemoryPool *pmp, CExpression *pexprArray, ULONG ul);

			// returns the scalar array expression child of CScalarArrayComp
			static
			CExpression *PexprScalarArrayChild(CExpression *pexpr);

			// returns if the scalar array has all constant elements or children
			static
			BOOL FScalarConstArray(CExpression *pexpr);

			// returns if the scalar constant array has already been collapased
			static
			BOOL FScalarArrayCollapsed(CExpression *pexprArray);

			// returns true if the subquery is a ScalarSubqueryAny
			static
			BOOL FAnySubquery(COperator *pop);

			// returns the expression under the Nth project element of a CLogicalProject
			static
			CExpression *PNthProjectElementExpr(CExpression *pexpr, ULONG ul);

			// check if the Project list has an inner reference assuming project list has one projecet element
			static
			BOOL FInnerRefInProjectList(CExpression *pexpr);

			// Check if expression tree has a col being referenced in the CColRefSet passed as input
			static
			BOOL FExprHasAnyCrFromCrs(CExpression *pexpr, CColRefSet *pcrs);

			// If it's a scalar array of all CScalarConst, collapse it into a single
			// expression but keep the constants in the operator.
			static
			CExpression *PexprCollapseConstArray(IMemoryPool *pmp, CExpression *pexprArray);

			// check if expression is scalar array coerce
			static
			BOOL FScalarArrayCoerce(CExpression *pexpr);

			// is the given expression a scalar identifier with the given column reference
			static
			BOOL FScalarIdent(CExpression *pexpr, CColRef *pcr);

			// check if expression is scalar const
			static
			BOOL FScalarConst(CExpression *pexpr);
			
			// check if this is a variable-free expression
			static
			BOOL FVarFreeExpr(CExpression *pexpr);

			// check if expression is a predicate
			static
			BOOL FPredicate(CExpression *pexpr);

			// is this type supported in contradiction detection using stats logic
			static
			BOOL FIntType(IMDId *pmdidType);

			// is this type supported in contradiction detection
			static
			BOOL FConstrainableType(IMDId *pmdidType);

			// check if a binary operator uses only columns produced by its children
			static
			BOOL FUsesChildColsOnly(CExpressionHandle &exprhdl);

			// check if inner child of a binary operator uses columns not produced by outer child
			static
			BOOL FInnerUsesExternalCols(CExpressionHandle &exprhdl);

			// check if inner child of a binary operator uses only columns not produced by outer child
			static
			BOOL FInnerUsesExternalColsOnly(CExpressionHandle &exprhdl);

			// check if comparison operators are available for the given columns
			static
			BOOL FComparisonPossible(DrgPcr *pdrgpcr, IMDType::ECmpType ecmpt);

			static
			ULONG UlCountOperator(const CExpression *pexpr, COperator::EOperatorId eopid);

			// return the max subset of redistributable columns for the given columns
			static
			DrgPcr *PdrgpcrRedistributableSubset(IMemoryPool *pmp, DrgPcr *pdrgpcr);

			// check if hashing is possible for the given columns
			static
			BOOL FHashable(DrgPcr *pdrgpcr);

			// check if the given operator is a logical DML operator
			static
			BOOL FLogicalDML(COperator *pop);

			// return regular string from wide-character string
			static
			CHAR *SzFromWsz(IMemoryPool *pmp, WCHAR *wsz);

			// return column reference defined by project element
			static
			CColRef *PcrFromProjElem(CExpression *pexprPrEl);
			
			// is given column functionally dependent on the given keyset
			static
			BOOL FFunctionallyDependent(IMemoryPool *pmp, CDrvdPropRelational *pdprel, CColRefSet *pcrsKey, CColRef *pcr);

			// construct an array of colids from the given array of column references
			static
			DrgPul *Pdrgpul(IMemoryPool *pmp, const DrgPcr *pdrgpcr);

			// generate a timestamp-based file name
			static
			void GenerateFileName
				(
				CHAR *szBuf,
				const CHAR *szPrefix,
				const CHAR *szExt,
				ULONG ulLength,
				ULONG ulSessionId,
				ULONG ulCmdId
				);

			// return the mapping of the given colref based on the given hashmap
			static
			CColRef *PcrRemap(const CColRef *pcr, HMUlCr *phmulcr, BOOL fMustExist);

			// create a new colrefset corresponding to the given colrefset
			// and based on the given mapping
			static
			CColRefSet *PcrsRemap(IMemoryPool *pmp, CColRefSet *pcrs, HMUlCr *phmulcr, BOOL fMustExist);

			// create an array of column references corresponding to the given array
			// and based on the given mapping
			static
			DrgPcr *PdrgpcrRemap(IMemoryPool *pmp, DrgPcr *pdrgpcr, HMUlCr *phmulcr, BOOL fMustExist);

			// create an array of column references corresponding to the given array
			// and based on the given mapping and create new colrefs if necessary
			static
			DrgPcr *PdrgpcrRemapAndCreate(IMemoryPool *pmp, DrgPcr *pdrgpcr, HMUlCr *phmulcr);

			// create an array of column arrays corresponding to the given array
			// and based on the given mapping
			static
			DrgDrgPcr *PdrgpdrgpcrRemap(IMemoryPool *pmp, DrgDrgPcr *pdrgpdrgpcr, HMUlCr *phmulcr, BOOL fMustExist);

			// remap given array of expressions with provided column mappings
			static
			DrgPexpr *PdrgpexprRemap(IMemoryPool *pmp, DrgPexpr *pdrgpexpr, HMUlCr *phmulcr);
			
			// create ColRef->ColRef mapping using the given ColRef arrays
			static
			HMUlCr *PhmulcrMapping(IMemoryPool *pmp, DrgPcr *pdrgpcrFrom, DrgPcr *pdrgpcrTo);

			// add col ID->ColRef mappings to the given hashmap based on the
			// given ColRef arrays
			static
			void AddColumnMapping(IMemoryPool *pmp, HMUlCr *phmulcr, DrgPcr *pdrgpcrFrom, DrgPcr *pdrgpcrTo);

			// create a copy of the array of column references
			static
			DrgPcr *PdrgpcrExactCopy(IMemoryPool *pmp, DrgPcr *pdrgpcr);

			// create an array of new column references with the same names and
			// types as the given column references.
			// if the passed map is not null, mappings from old to copied variables are added to it
			static
			DrgPcr *PdrgpcrCopy(IMemoryPool *pmp, DrgPcr *pdrgpcr, BOOL fAllComputed = false, HMUlCr *phmulcr = NULL);

			// equality check between two arrays of column refs. Inputs can be NULL
			static
			BOOL FEqual(DrgPcr *pdrgpcrFst, DrgPcr *pdrgpcrSnd);

			// compute hash value for an array of column references
			static
			ULONG UlHashColArray(const DrgPcr *pdrgpcr, const ULONG ulMaxCols = 5);

			// return the set of column reference from the CTE Producer corresponding to the
			// subset of input columns from the CTE Consumer
			static
			CColRefSet *PcrsCTEProducerColumns(IMemoryPool *pmp, CColRefSet *pcrsInput, CLogicalCTEConsumer *popCTEConsumer);

			// construct the join condition (AND-tree of INDF operators)
			// from the array of input columns reference arrays (aligned)
			static
			CExpression *PexprConjINDFCond(IMemoryPool *pmp, DrgDrgPcr *pdrgpdrgpcrInput);

			// check whether a colref array contains repeated items
			static
			BOOL FHasDuplicates(const DrgPcr *pdrgpcr);

			// cast the input expression to the destination mdid
			static
			CExpression *PexprCast(IMemoryPool *pmp, CMDAccessor *pmda, CExpression *pexpr, IMDId *pmdidDest);

			// construct a logical join expression of the given type, with the given children
			static
			CExpression *PexprLogicalJoin(IMemoryPool *pmp, EdxlJoinType edxljointype, DrgPexpr *pdrgpexpr);

			// construct an array of scalar ident expressions from the given array
			// of column references
			static
			DrgPexpr *PdrgpexprScalarIdents(IMemoryPool *pmp, DrgPcr *pdrgpcr);

			// return the columns from the scalar ident expressions in the given array
			static
			CColRefSet *PcrsExtractColumns(IMemoryPool *pmp, const DrgPexpr *pdrgpexpr);

			// create a new bitset of the given length, where all the bits are set
			static
			CBitSet *PbsAllSet(IMemoryPool *pmp, ULONG ulSize);

			// return a new bitset, setting the bits in the given array
			static
			CBitSet *Pbs(IMemoryPool *pmp, DrgPul *pdrgpul);

			// create a hashmap of constraints corresponding to a bool const on the given partkeys
			static
			HMUlCnstr *PhmulcnstrBoolConstOnPartKeys
				(
				IMemoryPool *pmp,
				DrgDrgPcr *pdrgpdrgpcrPartKey,
				BOOL fVal
				);

			// extract part constraint from metadata
			// if 'pmdpartcnstr' is not NULL and 'fDummyConstraint' is true, then the partition constraint
			// will be marked as `dummy' and the constraint expression will not be computed
			static
			CPartConstraint *PpartcnstrFromMDPartCnstr
				(
				IMemoryPool *pmp,
				CMDAccessor *pmda,
				DrgDrgPcr *pdrgpdrgpcrPartKey,
				const IMDPartConstraint *pmdpartcnstr,
				DrgPcr *pdrgpcrOutput,
				BOOL fDummyConstraint = false
				);

			// helper to create a dummy constant table expression
			static
			CExpression *PexprLogicalCTGDummy(IMemoryPool *pmp);

			// map a column from source array to destination array based on position
			static
			CColRef *PcrMap(CColRef *pcrSource, DrgPcr *pdrgpcrSource, DrgPcr *pdrgpcrTarget);

			// check if group expression is a motion and there is an unresolved consumer
			// not specified in the required properties
			static
			BOOL FMotionOverUnresolvedPartConsumers
				(
				IMemoryPool *pmp,
				CExpressionHandle &exprhdl,
				CPartIndexMap *ppimReqd
				);

			//	return index of the set containing given column
			static
			ULONG UlPcrIndexContainingSet(DrgPcrs *pdrgpcrs, const CColRef *pcr);

			// collapse the top two project nodes, if unable return NULL
			static
			CExpression *PexprCollapseProjects(IMemoryPool *pmp, CExpression *pexpr);

			// match function between index get/scan operators
			template <class T>
			static
			BOOL FMatchIndex(T *pop1, COperator *pop2);

			// match function between dynamic index get/scan operators
			template <class T>
			static
			BOOL FMatchDynamicIndex(T *pop1, COperator *pop2);

			// match function between dynamic get/scan operators
			template <class T>
			static
			BOOL FMatchDynamicScan(T *pop1, COperator *pop2);

			// match function between dynamic bitmap get/scan operators
			template <class T>
			static
			BOOL FMatchDynamicBitmapScan(T *pop1, COperator *pop2);

			// match function between bitmap get/scan operators
			template <class T>
			static
			BOOL FMatchBitmapScan(T *pop1, COperator *pop2);

			// compares two Idatums, useful for sorting functions
			static
			INT IDatumCmp(const void *pv1, const void *pv2);

			// check if the equivalance classes are disjoint
			static
			BOOL FEquivalanceClassesDisjoint(IMemoryPool *pmp, const DrgPcrs *pdrgpcrs);

			// check if the equivalance classes are same
			static
			BOOL FEquivalanceClassesEqual(IMemoryPool *pmp, DrgPcrs *pdrgpcrsFst, DrgPcrs *pdrgpcrsSnd);

			// get execution locality
			static
			EExecLocalityType ExecLocalityType(CDistributionSpec *pds);

			// generate a limit expression on top of the given relational child with the given offset and limit count
			static CExpression *PexprLimit(IMemoryPool *pmp, CExpression *pexpr, ULONG ulOffSet, ULONG ulCount);

			// generate part oid
			static
			BOOL FGeneratePartOid(IMDId *pmdid);

			// return true if given expression contains window aggregate function
			static
			BOOL FHasAggWindowFunc(CExpression *pexpr);

			// return true if the given expression is a cross join
			static
			BOOL FCrossJoin(CExpression *pexpr);

			// extract scalar ident column reference from scalar expression containing
			// only one scalar ident in the tree
			const static
			CColRef *PcrExtractFromScExpression(CExpression *pexpr);
	}; // class CUtils

	// hash set from expressions
	typedef CHashSet<CExpression, CExpression::UlHashDedup, CUtils::FEqual, CleanupRelease<CExpression> > HSExpr;


	//---------------------------------------------------------------------------
	//	@function:
	//		CUtils::PexprLogicalJoin
	//
	//	@doc:
	//		Generate a join expression from given expressions
	//
	//---------------------------------------------------------------------------
	template<class T>
	CExpression *
	CUtils::PexprLogicalJoin
		(
		IMemoryPool *pmp,
		CExpression *pexprLeft,
		CExpression *pexprRight,
		CExpression *pexprPredicate
		)
	{
		GPOS_ASSERT(NULL != pexprLeft);
		GPOS_ASSERT(NULL != pexprRight);
		GPOS_ASSERT(NULL != pexprPredicate);

		return GPOS_NEW(pmp) CExpression
				(
				pmp,
				GPOS_NEW(pmp) T(pmp),
				pexprLeft,
				pexprRight,
				pexprPredicate
				);
	}

	//---------------------------------------------------------------------------
	//	@function:
	//		CUtils::PexprLogicalApply
	//
	//	@doc:
	//		Generate an apply expression from given expressions
	//
	//---------------------------------------------------------------------------
	template<class T>
	CExpression *
	CUtils::PexprLogicalApply
		(
		IMemoryPool *pmp,
		CExpression *pexprLeft,
		CExpression *pexprRight,
		CExpression *pexprPred
		)
	{
		GPOS_ASSERT(NULL != pexprLeft);
		GPOS_ASSERT(NULL != pexprRight);

		CExpression *pexprScalar = pexprPred;
		if (NULL == pexprPred)
		{
			pexprScalar = PexprScalarConstBool(pmp, true /*fVal*/);
		}

		return GPOS_NEW(pmp) CExpression
				(
				pmp,
				GPOS_NEW(pmp) T(pmp),
				pexprLeft,
				pexprRight,
				pexprScalar
				);
	}


	//---------------------------------------------------------------------------
	//	@function:
	//		CUtils::PexprLogicalApply
	//
	//	@doc:
	//		Generate an apply expression with a known inner column
	//
	//---------------------------------------------------------------------------
	template<class T>
	CExpression *
	CUtils::PexprLogicalApply
		(
		IMemoryPool *pmp,
		CExpression *pexprLeft,
		CExpression *pexprRight,
		const CColRef *pcrInner,
		COperator::EOperatorId eopidOriginSubq,
		CExpression *pexprPred
		)
	{
		GPOS_ASSERT(NULL != pexprLeft);
		GPOS_ASSERT(NULL != pexprRight);
		GPOS_ASSERT(NULL != pcrInner);

		CExpression *pexprScalar = pexprPred;
		if (NULL == pexprPred)
		{
			pexprScalar = PexprScalarConstBool(pmp, true /*fVal*/);
		}

		DrgPcr *pdrgpcr = GPOS_NEW(pmp) DrgPcr(pmp);
		pdrgpcr->Append(const_cast<CColRef *>(pcrInner));
		return GPOS_NEW(pmp) CExpression
				(
				pmp,
				GPOS_NEW(pmp) T(pmp, pdrgpcr, eopidOriginSubq),
				pexprLeft,
				pexprRight,
				pexprScalar
				);
	}

	//---------------------------------------------------------------------------
	//	@function:
	//		CUtils::PexprLogicalApply
	//
	//	@doc:
	//		Generate an apply expression with known array of inner columns
	//
	//---------------------------------------------------------------------------
	template<class T>
	CExpression *
	CUtils::PexprLogicalApply
		(
		IMemoryPool *pmp,
		CExpression *pexprLeft,
		CExpression *pexprRight,
		DrgPcr *pdrgpcrInner,
		COperator::EOperatorId eopidOriginSubq,
		CExpression *pexprPred
		)
	{
		GPOS_ASSERT(NULL != pexprLeft);
		GPOS_ASSERT(NULL != pexprRight);
		GPOS_ASSERT(NULL != pdrgpcrInner);
		GPOS_ASSERT(0 < pdrgpcrInner->UlLength());

		CExpression *pexprScalar = pexprPred;
		if (NULL == pexprPred)
		{
			pexprScalar = PexprScalarConstBool(pmp, true /*fVal*/);
		}

		return GPOS_NEW(pmp) CExpression
				(
				pmp,
				GPOS_NEW(pmp) T(pmp, pdrgpcrInner, eopidOriginSubq),
				pexprLeft,
				pexprRight,
				pexprScalar
				);
	}

	//---------------------------------------------------------------------------
	//	@class:
	//		CUtils::PexprLogicalCorrelatedQuantifiedApply
	//
	//	@doc:
	//		Helper to create a left semi correlated apply from left semi apply
	//
	//---------------------------------------------------------------------------
	template<class T>
	CExpression *
	CUtils::PexprLogicalCorrelatedQuantifiedApply
		(
		IMemoryPool *pmp,
		CExpression *pexprLeft,
		CExpression *pexprRight,
		DrgPcr *pdrgpcrInner,
		COperator::EOperatorId eopidOriginSubq,
		CExpression *pexprPred
		)
	{
		GPOS_ASSERT(NULL != pexprLeft);
		GPOS_ASSERT(NULL != pexprRight);
		GPOS_ASSERT(NULL != pdrgpcrInner);
		GPOS_ASSERT(0 < pdrgpcrInner->UlLength());

		CExpression *pexprScalar = pexprPred;
		if (NULL == pexprPred)
		{
			pexprScalar = PexprScalarConstBool(pmp, true /*fVal*/);
		}

		if (COperator::EopLogicalSelect != pexprRight->Pop()->Eopid())
		{
			// quantified comparison was pushed down, we create a dummy comparison here
			GPOS_ASSERT(!CUtils::FHasOuterRefs(pexprRight) &&
				"unexpected outer references in inner child of Semi Apply expression ");
			pexprScalar->Release();
			pexprScalar = PexprScalarConstBool(pmp, true /*fVal*/);
		}
		else
		{
			// quantified comparison is now on top of inner expression, skip to child
			(*pexprRight)[1]->AddRef();
			CExpression *pexprNewPredicate = (*pexprRight)[1];
			pexprScalar->Release();
			pexprScalar = pexprNewPredicate;

			(*pexprRight)[0]->AddRef();
			CExpression *pexprChild = (*pexprRight)[0];
			pexprRight->Release();
			pexprRight = pexprChild;
		}

		return GPOS_NEW(pmp) CExpression
				(
				pmp,
				GPOS_NEW(pmp) T(pmp, pdrgpcrInner, eopidOriginSubq),
				pexprLeft,
				pexprRight,
				pexprScalar
				);
	}

	//---------------------------------------------------------------------------
	//	@function:
	//		CUtils::AddRefAppend
	//
	//	@doc:
	//		Append elements from input array to output array, starting from
	//		given index, after add-refing them
	//
	//---------------------------------------------------------------------------
	template <class T, void (*pfnDestroy)(T*)>
	void
	CUtils::AddRefAppend
		(
		CDynamicPtrArray<T, pfnDestroy> *pdrgptOutput,
		CDynamicPtrArray<T, pfnDestroy> *pdrgptInput,
		ULONG ulStart
		)
	{
		GPOS_ASSERT(NULL != pdrgptOutput);
		GPOS_ASSERT(NULL != pdrgptInput);

		const ULONG ulSize = pdrgptInput->UlLength();
		GPOS_ASSERT_IMP(0 < ulSize, ulStart < ulSize);

		for (ULONG ul = ulStart; ul < ulSize; ul++)
		{
			T *pt = (*pdrgptInput)[ul];
			CRefCount *prc = dynamic_cast<CRefCount *>(pt);
			prc->AddRef();
			pdrgptOutput->Append(pt);
		}
	}

	//---------------------------------------------------------------------------
	//	@function:
	//		CUtils::FScalarConstInt
	//
	//	@doc:
	//		Check if the given expression is an INT,
	//		the template parameter is an INT type
	//
	//---------------------------------------------------------------------------
	template<class T>
	BOOL
	CUtils::FScalarConstInt
		(
		CExpression *pexpr
		)
	{
		GPOS_ASSERT(NULL != pexpr);

		IMDType::ETypeInfo eti = T::EtiType();
		GPOS_ASSERT(IMDType::EtiInt2 == eti || IMDType::EtiInt4 == eti || IMDType::EtiInt8 == eti);

		COperator *pop = pexpr->Pop();
		if (COperator::EopScalarConst == pop->Eopid())
		{
			CScalarConst *popScalarConst = CScalarConst::PopConvert(pop);
			if (eti == popScalarConst->Pdatum()->Eti())
			{
				return true;
			}
		}

		return false;
	}

	//---------------------------------------------------------------------------
	//	@function:
	//		CUtils::FMatchIndex
	//
	//	@doc:
	//		Match function between index get/scan operators
	//
	//---------------------------------------------------------------------------
	template<class T>
	BOOL
	CUtils::FMatchIndex
		(
		T *pop1,
		COperator *pop2
		)
	{
		if (pop1->Eopid() != pop2->Eopid())
		{
			return false;
		}

		T *popIndex = T::PopConvert(pop2);

		return pop1->UlOriginOpId() == popIndex->UlOriginOpId() &&
				pop1->Ptabdesc()->Pmdid()->FEquals(popIndex->Ptabdesc()->Pmdid()) &&
				pop1->Pindexdesc()->Pmdid()->FEquals(popIndex->Pindexdesc()->Pmdid()) &&
				pop1->PdrgpcrOutput()->FEqual(popIndex->PdrgpcrOutput());
	}

	//---------------------------------------------------------------------------
	//	@function:
	//		CUtils::FMatchDynamicIndex
	//
	//	@doc:
	//		Match function between dynamic index get/scan operators
	//
	//---------------------------------------------------------------------------
	template<class T>
	BOOL
	CUtils::FMatchDynamicIndex
		(
		T *pop1,
		COperator *pop2
		)
	{
		if (pop1->Eopid() != pop2->Eopid())
		{
			return false;
		}

		T *popIndex2 = T::PopConvert(pop2);

		// match if the index descriptors are identical
		// we will compare MDIds, so both indexes should be partial or non-partial.
		// for heterogeneous indexes, we use pointer comparison of part constraints to avoid
		// memory allocation because matching function is used while holding spin locks.
		// this means that we may miss matches for heterogeneous indexes
		return  pop1->UlOriginOpId() == popIndex2->UlOriginOpId() &&
				pop1->UlScanId() == popIndex2->UlScanId() &&
				pop1->UlSecondaryScanId() == popIndex2->UlSecondaryScanId() &&
				pop1->Ptabdesc()->Pmdid()->FEquals(popIndex2->Ptabdesc()->Pmdid()) &&
				pop1->Pindexdesc()->Pmdid()->FEquals(popIndex2->Pindexdesc()->Pmdid()) &&
				pop1->PdrgpcrOutput()->FEqual(popIndex2->PdrgpcrOutput()) &&
				(!pop1->FPartial() || (pop1->Ppartcnstr() == popIndex2->Ppartcnstr()));
	}

	//---------------------------------------------------------------------------
	//	@function:
	//		CUtils::FMatchDynamicScan
	//
	//	@doc:
	//		Match function between dynamic get/scan operators
	//
	//---------------------------------------------------------------------------
	template<class T>
	BOOL
	CUtils::FMatchDynamicScan
		(
		T *pop1,
		COperator *pop2
		)
	{
		if (pop1->Eopid() != pop2->Eopid())
		{
			return false;
		}

		T *popScan2 = T::PopConvert(pop2);

		// match if the table descriptors are identical
		// for partial scans, we use pointer comparison of part constraints to avoid
		// memory allocation because matching function is used while holding spin locks.
		// this means that we may miss matches for partial scans
		return pop1->UlScanId() == popScan2->UlScanId() &&
				pop1->UlSecondaryScanId() == popScan2->UlSecondaryScanId() &&
				pop1->Ptabdesc()->Pmdid()->FEquals(popScan2->Ptabdesc()->Pmdid()) &&
				pop1->PdrgpcrOutput()->FEqual(popScan2->PdrgpcrOutput()) &&
				((!pop1->FPartial() && !popScan2->FPartial()) ||
				 (pop1->Ppartcnstr() == popScan2->Ppartcnstr()));
	}


	//---------------------------------------------------------------------------
	//	@function:
	//		CUtils::FMatchDynamicBitmapScan
	//
	//	@doc:
	//		Match function between dynamic bitmap get/scan operators
	//
	//---------------------------------------------------------------------------
	template<class T>
	BOOL
	CUtils::FMatchDynamicBitmapScan
		(
		T *pop1,
		COperator *pop2
		)
	{
		if (pop1->Eopid() != pop2->Eopid())
		{
			return false;
		}

		T *popDynamicBitmapScan2 = T::PopConvert(pop2);

		return
			pop1->UlOriginOpId() == popDynamicBitmapScan2->UlOriginOpId() &&
			FMatchDynamicScan(pop1, pop2);	// call match dynamic scan to compare other member vars
	}


	//---------------------------------------------------------------------------
	//	@function:
	//		CUtils::FMatchBitmapScan
	//
	//	@doc:
	//		Match function between bitmap get/scan operators
	//
	//---------------------------------------------------------------------------
	template<class T>
	BOOL
	CUtils::FMatchBitmapScan
		(
		T *pop1,
		COperator *pop2
		)
	{
		if (pop1->Eopid() != pop2->Eopid())
		{
			return false;
		}

		T *popScan2 = T::PopConvert(pop2);

		return pop1->UlOriginOpId() == popScan2->UlOriginOpId() &&
			pop1->Ptabdesc()->Pmdid()->FEquals(popScan2->Ptabdesc()->Pmdid()) &&
			pop1->PdrgpcrOutput()->FEqual(popScan2->PdrgpcrOutput());
	}
} // namespace gpopt


#ifdef GPOS_DEBUG

// helper to print given expression
// outside of namespace to make sure gdb can resolve the symbol easily
void PrintExpr(void *pv);

// helper to print memo structure
void PrintMemo(void *pv);

#endif // GPOS_DEBUG

#endif // !GPOPT_CUtils_H

// EOF
