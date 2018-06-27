//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright 2012 EMC Corp.
//
//	@filename:
//		CXformUtils.h
//
//	@doc:
//		Utility functions for xforms
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformUtils_H
#define GPOPT_CXformUtils_H

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/base/CColRef.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXform.h"

namespace gpopt
{

	using namespace gpos;

	// forward declarations
	class CGroupExpression;
	class CColRefSet;
	class CExpression;
	class CLogical;
	class CLogicalDynamicGet;
	class CPartConstraint;
	class CTableDescriptor;
	
	// structure describing a candidate for a partial dynamic index scan
	struct SPartDynamicIndexGetInfo
	{
		// md index
		const IMDIndex *m_pmdindex;

		// part constraint
		CPartConstraint *m_ppartcnstr;
		
		// index predicate expressions
		DrgPexpr *m_pdrgpexprIndex;

		// residual expressions
		DrgPexpr *m_pdrgpexprResidual;

		// ctor
		SPartDynamicIndexGetInfo
			(
			const IMDIndex *pmdindex,
			CPartConstraint *ppartcnstr,
			DrgPexpr *pdrgpexprIndex,
			DrgPexpr *pdrgpexprResidual
			)
			:
			m_pmdindex(pmdindex),
			m_ppartcnstr(ppartcnstr),
			m_pdrgpexprIndex(pdrgpexprIndex),
			m_pdrgpexprResidual(pdrgpexprResidual)
		{
			GPOS_ASSERT(NULL != ppartcnstr);

		}

		// dtor
		~SPartDynamicIndexGetInfo()
		{
			m_ppartcnstr->Release();
			CRefCount::SafeRelease(m_pdrgpexprIndex);
			CRefCount::SafeRelease(m_pdrgpexprResidual);
		}
	};

	// arrays over partial dynamic index get candidates
	typedef CDynamicPtrArray<SPartDynamicIndexGetInfo, CleanupDelete> DrgPpartdig;
	typedef CDynamicPtrArray<DrgPpartdig, CleanupRelease> DrgPdrgPpartdig;

	// map of expression to array of expressions
	typedef CHashMap<CExpression, DrgPexpr, CExpression::UlHash, CUtils::FEqual,
		CleanupRelease<CExpression>, CleanupRelease<DrgPexpr> > HMExprDrgPexpr;

	// iterator of map of expression to array of expressions
	typedef CHashMapIter<CExpression, DrgPexpr, CExpression::UlHash, CUtils::FEqual,
		CleanupRelease<CExpression>, CleanupRelease<DrgPexpr> > HMExprDrgPexprIter;

	// array of array of expressions
	typedef CDynamicPtrArray<DrgPexpr, CleanupRelease> DrgPdrgPexpr;

	//---------------------------------------------------------------------------
	//	@class:
	//		CXformUtils
	//
	//	@doc:
	//		Utility functions for xforms
	//
	//---------------------------------------------------------------------------
	class CXformUtils
	{
		private:
			
			// enum marking the index column types
			enum
			EIndexCols
			{
				EicKey,
				EicIncluded
			};

			typedef CLogical *(*PDynamicIndexOpConstructor)
						(
						IMemoryPool *pmp,
						const IMDIndex *pmdindex,
						CTableDescriptor *ptabdesc,
						ULONG ulOriginOpId,
						CName *pname,
						ULONG ulPartIndex,
						DrgPcr *pdrgpcrOutput,
						DrgDrgPcr *pdrgpdrgpcrPart,
						ULONG ulSecondaryPartIndexId,
						CPartConstraint *ppartcnstr,
						CPartConstraint *ppartcnstrRel
						);

			typedef CLogical *(*PStaticIndexOpConstructor)
						(
						IMemoryPool *pmp,
						const IMDIndex *pmdindex,
						CTableDescriptor *ptabdesc,
						ULONG ulOriginOpId,
						CName *pname,
						DrgPcr *pdrgpcrOutput
						);

			typedef CExpression *(PRewrittenIndexPath)
						(
						IMemoryPool *pmp,
						CExpression *pexprIndexCond,
						CExpression *pexprResidualCond,
						const IMDIndex *pmdindex,
						CTableDescriptor *ptabdesc,
						COperator *popLogical
						);

			// private copy ctor
			CXformUtils(const CXformUtils &);

			// create a logical assert for the not nullable columns of the given table
			// on top of the given child expression
			static
			CExpression *PexprAssertNotNull
				(
				IMemoryPool *pmp,
				CExpression *pexprChild,
				CTableDescriptor *ptabdesc,
				DrgPcr *pdrgpcr
				);

			// create a logical assert for the check constraints on the given table
			static
			CExpression *PexprAssertCheckConstraints
				(
				IMemoryPool *pmp,
				CExpression *pexprChild,
				CTableDescriptor *ptabdesc,
				DrgPcr *pdrgpcr
				);

			// add a min(col) project element to the given expression list for
			// each column in the given column array
			static
			void AddMinAggs
				(
				IMemoryPool *pmp,
				CMDAccessor *pmda,
				CColumnFactory *pcf,
				DrgPcr *pdrgpcr,
				HMCrCr *phmcrcr,
				DrgPexpr *pdrgpexpr,
				DrgPcr **ppdrgpcrNew
				);

			// check if all columns support MIN aggregate
			static
			BOOL FSupportsMinAgg(DrgPcr *pdrgpcr);

			// helper for extracting foreign key
			static
			CColRefSet *PcrsFKey
				(
				IMemoryPool *pmp,
				DrgPexpr *pdrgpexpr,
				CColRefSet *prcsOutput,
				CColRefSet *pcrsKey
				);

			// return the set of columns from the given array of columns which appear
			// in the index included / key columns
			static
			CColRefSet *PcrsIndexColumns
				(
				IMemoryPool *pmp,
				DrgPcr *pdrgpcr,
				const IMDIndex *pmdindex,
				const IMDRelation *pmdrel,
				EIndexCols eic
				);
			
			// return the ordered array of columns from the given array of columns which appear
			// in the index included / key columns
			static
			DrgPcr *PdrgpcrIndexColumns
				(
				IMemoryPool *pmp,
				DrgPcr *pdrgpcr,
				const IMDIndex *pmdindex,
				const IMDRelation *pmdrel,
				EIndexCols eic
				);

			// lookup hash join keys in scalar child group
			static
			void LookupHashJoinKeys(IMemoryPool *pmp, CExpression *pexpr, DrgPexpr **ppdrgpexprOuter, DrgPexpr **ppdrgpexprInner);

			// cache hash join keys on scalar child group
			static
			void CacheHashJoinKeys(CExpression *pexpr, DrgPexpr *pdrgpexprOuter, DrgPexpr *pdrgpexprInner);

			// helper to extract equality from a given expression
			static
			BOOL FExtractEquality
				(
				CExpression *pexpr,
				CExpression **ppexprEquality, // output: extracted equality expression, set to NULL if extraction failed
				CExpression **ppexprOther // output: sibling of equality expression, set to NULL if extraction failed
				);

			// check if given xform id is in the given array of xforms
			static
			BOOL FXformInArray(CXform::EXformId exfid, CXform::EXformId rgXforms[], ULONG ulXforms);

#ifdef GPOS_DEBUG
			// check whether the given join type is swapable
			static
			BOOL FSwapableJoinType(COperator::EOperatorId eopid);
#endif // GPOS_DEBUG

			// helper function for adding hash join alternative
			template <class T>
			static
			void AddHashJoinAlternative
				(
				IMemoryPool *pmp,
				CExpression *pexprJoin,
				DrgPexpr *pdrgpexprOuter,
				DrgPexpr *pdrgpexprInner,
				CXformResult *pxfres
				);

			// helper for transforming SubqueryAll into aggregate subquery
			static
			void SubqueryAllToAgg
				(
				IMemoryPool *pmp,
				CExpression *pexprSubquery,
				CExpression **ppexprNewSubquery, // output argument for new scalar subquery
				CExpression **ppexprNewScalar   // output argument for new scalar expression
				);

			// helper for transforming SubqueryAny into aggregate subquery
			static
			void SubqueryAnyToAgg
				(
				IMemoryPool *pmp,
				CExpression *pexprSubquery,
				CExpression **ppexprNewSubquery, // output argument for new scalar subquery
				CExpression **ppexprNewScalar   // output argument for new scalar expression
				);

			// create the Gb operator to be pushed below a join
			static
			CLogicalGbAgg *PopGbAggPushableBelowJoin
				(
				IMemoryPool *pmp,
				CLogicalGbAgg *popGbAggOld,
				CColRefSet *prcsOutput,
				CColRefSet *pcrsGrpCols
				);

			// check if the preconditions for pushing down Group by through join are satisfied
			static
			BOOL FCanPushGbAggBelowJoin
				(
				CColRefSet *pcrsGrpCols,
				CColRefSet *pcrsJoinOuterChildOutput,
				CColRefSet *pcrsJoinScalarUsedFromOuter,
				CColRefSet *pcrsGrpByOutput,
				CColRefSet *pcrsGrpByUsed,
				CColRefSet *pcrsFKey
				);

			// construct an expression representing a new access path using the given functors for
			// operator constructors and rewritten access path
			static
			CExpression *PexprBuildIndexPlan
				(
				IMemoryPool *pmp,
				CMDAccessor *pmda,
				CExpression *pexprGet,
				ULONG ulOriginOpId,
				DrgPexpr *pdrgpexprConds,
				CColRefSet *pcrsReqd,
				CColRefSet *pcrsScalarExpr,
				CColRefSet *pcrsOuterRefs,
				const IMDIndex *pmdindex,
				const IMDRelation *pmdrel,
				BOOL fAllowPartialIndex,
				CPartConstraint *ppcForPartialIndexes,
				IMDIndex::EmdindexType emdindtype,
				PDynamicIndexOpConstructor pdiopc,
				PStaticIndexOpConstructor psiopc,
				PRewrittenIndexPath prip
				);

			// create a dynamic operator for a btree index plan
			static
			CLogical *
			PopDynamicBtreeIndexOpConstructor
				(
				IMemoryPool *pmp,
				const IMDIndex *pmdindex,
				CTableDescriptor *ptabdesc,
				ULONG ulOriginOpId,
				CName *pname,
				ULONG ulPartIndex,
				DrgPcr *pdrgpcrOutput,
				DrgDrgPcr *pdrgpdrgpcrPart,
				ULONG ulSecondaryPartIndexId,
				CPartConstraint *ppartcnstr,
				CPartConstraint *ppartcnstrRel
				)
			{
				return GPOS_NEW(pmp) CLogicalDynamicIndexGet
						(
						pmp,
						pmdindex,
						ptabdesc,
						ulOriginOpId,
						pname,
						ulPartIndex,
						pdrgpcrOutput,
						pdrgpdrgpcrPart,
						ulSecondaryPartIndexId,
						ppartcnstr,
						ppartcnstrRel
						);
			}

			//	create a static operator for a btree index plan
			static
			CLogical *
			PopStaticBtreeIndexOpConstructor
				(
				IMemoryPool *pmp,
				const IMDIndex *pmdindex,
				CTableDescriptor *ptabdesc,
				ULONG ulOriginOpId,
				CName *pname,
				DrgPcr *pdrgpcrOutput
				)
			{
				return  GPOS_NEW(pmp) CLogicalIndexGet
						(
						pmp,
						pmdindex,
						ptabdesc,
						ulOriginOpId,
						pname,
						pdrgpcrOutput
						);
			}

			//	produce an expression representing a new btree index path
			static
			CExpression *
			PexprRewrittenBtreeIndexPath
				(
				IMemoryPool *pmp,
				CExpression *pexprIndexCond,
				CExpression *pexprResidualCond,
				const IMDIndex *,  // pmdindex
				CTableDescriptor *,  // ptabdesc
				COperator *popLogical
				)
			{
				// create the expression containing the logical index get operator
				return CUtils::PexprSafeSelect(pmp, GPOS_NEW(pmp) CExpression(pmp, popLogical, pexprIndexCond), pexprResidualCond);
			}

			// create a candidate dynamic get scan to suplement the partial index scans
			static
			SPartDynamicIndexGetInfo *PpartdigDynamicGet
				(
				IMemoryPool *pmp,
				DrgPexpr *pdrgpexprScalar,
				CPartConstraint *ppartcnstrCovered,
				CPartConstraint *ppartcnstrRel
				);

			// returns true iff the given expression is a Not operator whose child is a
			// scalar identifier
			static
			BOOL FNotIdent(CExpression *pexpr);

			// creates a condition of the form col = fVal, where col is the given column
			static
			CExpression *PexprEqualityOnBoolColumn
				(
				IMemoryPool *pmp,
				CMDAccessor *pmda,
				BOOL fNegated,
				CColRef *pcr
				);

			// construct a bitmap index path expression for the given predicate
			// out of the children of the given expression
			static
			CExpression *PexprBitmapFromChildren
				(
				IMemoryPool *pmp,
				CMDAccessor *pmda,
				CExpression *pexprOriginalPred,
				CExpression *pexprPred,
				CTableDescriptor *ptabdesc,
				const IMDRelation *pmdrel,
				DrgPcr *pdrgpcrOutput,
				CColRefSet *pcrsOuterRefs,
				CColRefSet *pcrsReqd,
				BOOL fConjunction,
				CExpression **ppexprRecheck,
				CExpression **ppexprResidual
				);

			// returns the recheck condition to use in a bitmap
			// index scan computed out of the expression 'pexprPred' that
			// uses the bitmap index
			// fBoolColumn (and fNegatedColumn) say whether the predicate is a
			// (negated) boolean scalar identifier
			// caller takes ownership of the returned expression
			static
			CExpression *PexprBitmapCondToUse
				(
				IMemoryPool *pmp,
				CMDAccessor *pmda,
				CExpression *pexprPred,
				BOOL fBoolColumn,
				BOOL fNegatedBoolColumn,
				CColRefSet *pcrsScalar
				);
			
			// compute the residual predicate for a bitmap table scan
			static
			void ComputeBitmapTableScanResidualPredicate
				(
				IMemoryPool *pmp, 
				BOOL fConjunction,
				CExpression *pexprOriginalPred,
				CExpression **ppexprResidual,
				DrgPexpr *pdrgpexprResidualNew
				);

			// compute a disjunction of two part constraints
			static
			CPartConstraint *PpartcnstrDisjunction
				(
				IMemoryPool *pmp,
				CPartConstraint *ppartcnstrOld,
				CPartConstraint *ppartcnstrNew
				);
			
			// construct a bitmap index path expression for the given predicate coming
			// from a condition without outer references
			static
			CExpression *PexprBitmapForSelectCondition
				(
				IMemoryPool *pmp,
				CMDAccessor *pmda,
				CExpression *pexprPred,
				CTableDescriptor *ptabdesc,
				const IMDRelation *pmdrel,
				DrgPcr *pdrgpcrOutput,
				CColRefSet *pcrsReqd,
				CExpression **ppexprRecheck,
				BOOL fBoolColumn,
				BOOL fNegatedBoolColumn
				);

			// construct a bitmap index path expression for the given predicate coming
			// from a condition with outer references that could potentially become
			// an index lookup
			static
			CExpression *PexprBitmapForIndexLookup
				(
				IMemoryPool *pmp,
				CMDAccessor *pmda,
				CExpression *pexprPred,
				CTableDescriptor *ptabdesc,
				const IMDRelation *pmdrel,
				DrgPcr *pdrgpcrOutput,
				CColRefSet *pcrsOuterRefs,
				CColRefSet *pcrsReqd,
				CExpression **ppexprRecheck
				);

			// if there is already an index probe in pdrgpexprBitmap on the same
			// index as the given pexprBitmap, modify the existing index probe and
			// the corresponding recheck conditions to subsume pexprBitmap and
			// pexprRecheck respectively
			static
			BOOL FMergeWithPreviousBitmapIndexProbe
				(
				IMemoryPool *pmp,
				CExpression *pexprBitmap,
				CExpression *pexprRecheck,
				DrgPexpr *pdrgpexprBitmap,
				DrgPexpr *pdrgpexprRecheck
				);

			// iterate over given hash map and return array of arrays of project elements sorted by the column id of the first entries
			static
			DrgPdrgPexpr *PdrgpdrgpexprSortedPrjElemsArray(IMemoryPool *pmp, HMExprDrgPexpr *phmexprdrgpexpr);

			// comparator used in sorting arrays of project elements based on the column id of the first entry
			static
			INT ICmpPrjElemsArr(const void *pvFst, const void *pvSnd);

		public:

			// helper function for implementation xforms on binary operators
			// with predicates (e.g. joins)
			template<class T>
			static
			void TransformImplementBinaryOp
				(
				CXformContext *pxfctxt,
				CXformResult *pxfres,
				CExpression *pexpr
				);

			// helper function for implementation of hash joins
			template <class T>
			static
			void ImplementHashJoin
				(
				CXformContext *pxfctxt,
				CXformResult *pxfres,
				CExpression *pexpr,
				BOOL fAntiSemiJoin = false
				);

			// helper function for implementation of nested loops joins
			template <class T>
			static
			void ImplementNLJoin
				(
				CXformContext *pxfctxt,
				CXformResult *pxfres,
				CExpression *pexpr
				);

			// helper for removing IsNotFalse join predicate for GPDB anti-semi hash join
			static
			BOOL FProcessGPDBAntiSemiHashJoin(IMemoryPool *pmp, CExpression *pexpr, CExpression **ppexprResult);

			// check the applicability of logical join to physical join xform
			static
			CXform::EXformPromise ExfpLogicalJoin2PhysicalJoin(CExpressionHandle &exprhdl);

			// check the applicability of semi join to cross product
			static
			CXform::EXformPromise ExfpSemiJoin2CrossProduct(CExpressionHandle &exprhdl);

			// check the applicability of N-ary join expansion
			static
			CXform::EXformPromise ExfpExpandJoinOrder(CExpressionHandle &exprhdl);

			// extract foreign key
			static
			CColRefSet *PcrsFKey
				(
				IMemoryPool *pmp,
				CExpression *pexprOuter,
				CExpression *pexprInner,
				CExpression *pexprScalar
				);
			
			// compute a swap of the two given joins
			static
			CExpression *PexprSwapJoins
				(
				IMemoryPool *pmp,
				CExpression *pexprTopJoin,
				CExpression *pexprBottomJoin
				);

			// push a Gb, optionally with a having clause below the child join
			static
			CExpression *PexprPushGbBelowJoin
				(
				IMemoryPool *pmp,
				CExpression *pexpr
				);

			// check if the the array of aligned input columns are of the same type
			static
			BOOL FSameDatatype(DrgDrgPcr *pdrgpdrgpcrInput);

			// helper function to separate subquery predicates in a top Select node
			static
			CExpression *PexprSeparateSubqueryPreds(IMemoryPool *pmp, CExpression *pexpr);

			// helper for creating inverse predicate for unnesting subquery ALL
			static
			CExpression *PexprInversePred(IMemoryPool *pmp, CExpression *pexprSubquery);

			// helper for creating a null indicator expression
			static
			CExpression *PexprNullIndicator(IMemoryPool *pmp, CExpression *pexpr);

			// helper for creating a logical DML on top of a project
			static
			CExpression *PexprLogicalDMLOverProject
				(
				IMemoryPool *pmp,
				CExpression *pexprChild,
				CLogicalDML::EDMLOperator edmlop,
				CTableDescriptor *ptabdesc,
				DrgPcr *pdrgpcr,
				CColRef *pcrCtid,
				CColRef *pcrSegmentId
				);

			// check whether there are any BEFORE or AFTER triggers on the
			// given table that match the given DML operation
			static
			BOOL FTriggersExist
				(
				CLogicalDML::EDMLOperator edmlop,
				CTableDescriptor *ptabdesc,
				BOOL fBefore
				);

			// does the given trigger type match the given logical DML type
			static
			BOOL FTriggerApplies
				(
				CLogicalDML::EDMLOperator edmlop,
				const IMDTrigger *pmdtrigger
				);

			// construct a trigger expression on top of the given expression
			static
			CExpression *PexprRowTrigger
				(
				IMemoryPool *pmp,
				CExpression *pexprChild,
				CLogicalDML::EDMLOperator edmlop,
				IMDId *pmdidRel,
				BOOL fBefore,
				DrgPcr *pdrgpcr
				);

			// construct a trigger expression on top of the given expression
			static
			CExpression *PexprRowTrigger
				(
				IMemoryPool *pmp,
				CExpression *pexprChild,
				CLogicalDML::EDMLOperator edmlop,
				IMDId *pmdidRel,
				BOOL fBefore,
				DrgPcr *pdrgpcrOld,
				DrgPcr *pdrgpcrNew
				);

			// construct a logical partition selector for the given table descriptor on top
			// of the given child expression. The partition selection filters use columns
			// from the given column array
			static
			CExpression *PexprLogicalPartitionSelector
				(
				IMemoryPool *pmp,
				CTableDescriptor *ptabdesc,
				DrgPcr *pdrgpcr,
				CExpression *pexprChild
				);

			// return partition filter expressions given a table
			// descriptor and the given column references
			static
			DrgPexpr *PdrgpexprPartEqFilters(IMemoryPool *pmp, CTableDescriptor *ptabdesc, DrgPcr *pdrgpcrSource);

			// helper for creating Agg expression equivalent to quantified subquery
			static
			void QuantifiedToAgg
				(
				IMemoryPool *pmp,
				CExpression *pexprSubquery,
				CExpression **ppexprNewSubquery,
				CExpression **ppexprNewScalar
				);

			// helper for creating Agg expression equivalent to existential subquery
			static
			void ExistentialToAgg
				(
				IMemoryPool *pmp,
				CExpression *pexprSubquery,
				CExpression **ppexprNewSubquery,
				CExpression **ppexprNewScalar
				);
			
			// create a logical assert for the check constraints on the given table
			static
			CExpression *PexprAssertConstraints
				(
				IMemoryPool *pmp,
				CExpression *pexprChild,
				CTableDescriptor *ptabdesc,
				DrgPcr *pdrgpcr
				);
			
			// create a logical assert for checking cardinality of update values
			static
			CExpression *PexprAssertUpdateCardinality
				(
				IMemoryPool *pmp,
				CExpression *pexprDMLChild,
				CExpression *pexprDML,
				CColRef *pcrCtid,
				CColRef *pcrSegmentId
				);

			// return true if stats derivation is needed for this xform
			static
			BOOL FDeriveStatsBeforeXform(CXform *pxform);

			// return true if xform is a subquery decorrelation xform
			static
			BOOL FSubqueryDecorrelation(CXform *pxform);

			// return true if xform is a subquery unnesting xform
			static
			BOOL FSubqueryUnnesting(CXform *pxform);

			// return true if xform should be applied to the next binding
			static
			BOOL FApplyToNextBinding(CXform *pxform, CExpression *pexprLastBinding);

			// return a formatted error message for the given exception
			static
			CWStringConst *PstrErrorMessage(IMemoryPool *pmp, ULONG ulMajor, ULONG ulMinor, ...);
			
			// return the array of key columns from the given array of columns which appear 
			// in the index key columns
			static
			DrgPcr *PdrgpcrIndexKeys
				(
				IMemoryPool *pmp,
				DrgPcr *pdrgpcr,
				const IMDIndex *pmdindex,
				const IMDRelation *pmdrel
				);
						
			// return the set of key columns from the given array of columns which appear 
			// in the index key columns
			static
			CColRefSet *PcrsIndexKeys
				(
				IMemoryPool *pmp,
				DrgPcr *pdrgpcr,
				const IMDIndex *pmdindex,
				const IMDRelation *pmdrel
				);
			
			// return the set of key columns from the given array of columns which appear 
			// in the index included columns
			static
			CColRefSet *PcrsIndexIncludedCols
				(
				IMemoryPool *pmp,
				DrgPcr *pdrgpcr,
				const IMDIndex *pmdindex,
				const IMDRelation *pmdrel
				);
			
			// check if an index is applicable given the required, output and scalar
			// expression columns
			static
			BOOL FIndexApplicable
				(
				IMemoryPool *pmp, 
				const IMDIndex *pmdindex,
				const IMDRelation *pmdrel,
				DrgPcr *pdrgpcrOutput, 
				CColRefSet *pcrsReqd, 
				CColRefSet *pcrsScalar,
				IMDIndex::EmdindexType emdindtype
				);
			
			// check whether a CTE should be inlined
			static
			BOOL FInlinableCTE(ULONG ulCTEId);

			// return the column reference of the n-th project element
			static
			CColRef *PcrProjectElement(CExpression *pexpr, ULONG ulIdxProjElement);

			// create an expression with "row_number" window function
			static
			CExpression *PexprRowNumber(IMemoryPool *pmp);

			// create a logical sequence project with a "row_number" window function
			static
			CExpression *PexprWindowWithRowNumber
				(
				IMemoryPool *pmp,
				CExpression *pexprWindowChild,
				DrgPcr *pdrgpcrInput
				);

			// generate a logical Assert expression that errors out when more than one row is generated
			static
			CExpression *PexprAssertOneRow
				(
				IMemoryPool *pmp,
				CExpression *pexprChild
				);

			// helper for adding CTE producer to global CTE info structure
			static
			CExpression *PexprAddCTEProducer
				(
				IMemoryPool *pmp,
				ULONG ulCTEId,
				DrgPcr *pdrgpcr,
				CExpression *pexpr
				);

			// does transformation generate an Apply expression
			static
			BOOL FGenerateApply
				(
				CXform::EXformId exfid
				)
			{
				return CXform::ExfSelect2Apply == exfid ||
						CXform::ExfProject2Apply == exfid ||
						CXform::ExfGbAgg2Apply == exfid ||
						CXform::ExfSubqJoin2Apply == exfid ||
						CXform::ExfSubqNAryJoin2Apply == exfid ||
						CXform::ExfSequenceProject2Apply == exfid;
			}

			// helper for creating IndexGet/DynamicIndexGet expression
			static
			CExpression *PexprLogicalIndexGet
				(
				IMemoryPool *pmp,
				CMDAccessor *pmda,
				CExpression *pexprGet,
				ULONG ulOriginOpId,
				DrgPexpr *pdrgpexprConds,
				CColRefSet *pcrsReqd,
				CColRefSet *pcrsScalarExpr,
				CColRefSet *pcrsOuterRefs,
				const IMDIndex *pmdindex,
				const IMDRelation *pmdrel,
				BOOL fAllowPartialIndex,
				CPartConstraint *ppcartcnstrIndex
				)
			{
				return PexprBuildIndexPlan
						(
						pmp,
						pmda,
						pexprGet,
						ulOriginOpId,
						pdrgpexprConds,
						pcrsReqd,
						pcrsScalarExpr,
						pcrsOuterRefs,
						pmdindex,
						pmdrel,
						fAllowPartialIndex,
						ppcartcnstrIndex,
						IMDIndex::EmdindBtree,
						PopDynamicBtreeIndexOpConstructor,
						PopStaticBtreeIndexOpConstructor,
						PexprRewrittenBtreeIndexPath
						);
			}
			
			// helper for creating bitmap bool op expressions
			static
			CExpression *PexprScalarBitmapBoolOp
				(
				IMemoryPool *pmp,
				CMDAccessor *pmda,
				CExpression *pexprOriginalPred,
				DrgPexpr *pdrgpexpr,
				CTableDescriptor *ptabdesc,
				const IMDRelation *pmdrel,
				DrgPcr *pdrgpcrOutput,
				CColRefSet *pcrsOuterRefs,
				CColRefSet *pcrsReqd,
				BOOL fConjunction,
				CExpression **ppexprRecheck,
				CExpression **ppexprResidual
				);
			
			// construct a bitmap bool op given the left and right bitmap access
			// path expressions
			static
			CExpression *PexprBitmapBoolOp
				(
				IMemoryPool *pmp, 
				IMDId *pmdidBitmapType, 
				CExpression *pexprLeft,
				CExpression *pexprRight,
				BOOL fConjunction
				);
			
			// construct a bitmap index path expression for the given predicate
			static
			CExpression *PexprBitmap
				(
				IMemoryPool *pmp, 
				CMDAccessor *pmda,
				CExpression *pexprOriginalPred,
				CExpression *pexprPred, 
				CTableDescriptor *ptabdesc,
				const IMDRelation *pmdrel,
				DrgPcr *pdrgpcrOutput,
				CColRefSet *pcrsOuterRefs,
				CColRefSet *pcrsReqd,
				BOOL fConjunction,
				CExpression **ppexprRecheck,
				CExpression **ppexprResidual
				);
			
			// given an array of predicate expressions, construct a bitmap access path
			// expression for each predicate and accumulate it in the pdrgpexprBitmap array
			static
			void CreateBitmapIndexProbeOps
				(
				IMemoryPool *pmp, 
				CMDAccessor *pmda,
				CExpression *pexprOriginalPred,
				DrgPexpr *pdrgpexpr, 
				CTableDescriptor *ptabdesc,
				const IMDRelation *pmdrel,
				DrgPcr *pdrgpcrOutput,
				CColRefSet *pcrsOuterRefs,
				CColRefSet *pcrsReqd,
				BOOL fConjunction,
				DrgPexpr *pdrgpexprBitmap,
				DrgPexpr *pdrgpexprRecheck,
				DrgPexpr *pdrgpexprResidual
				);

			// check if expression has any scalar node with ambiguous return type
			static
			BOOL FHasAmbiguousType(CExpression *pexpr, CMDAccessor *pmda);
			
			// construct a Bitmap(Dynamic)TableGet over BitmapBoolOp for the given
			// logical operator if bitmap indexes exist
			static
			CExpression *PexprBitmapTableGet
				(
				IMemoryPool *pmp,
				CLogical *popGet,
				ULONG ulOriginOpId,
				CTableDescriptor *ptabdesc,
				CExpression *pexprScalar,
				CColRefSet *pcrsOuterRefs,
				CColRefSet *pcrsReqd
				);

			// transform a Select over a (dynamic) table get into a bitmap table scan
			// over bitmap bool op
			static
			CExpression *PexprSelect2BitmapBoolOp
				(
				IMemoryPool *pmp,
				CExpression *pexpr
				);

			// find a set of partial index combinations
			static
			DrgPdrgPpartdig *PdrgpdrgppartdigCandidates
				(
				IMemoryPool *pmp,
				CMDAccessor *pmda,
				DrgPexpr *pdrgpexprScalar,
				DrgDrgPcr *pdrgpdrgpcrPartKey,
				const IMDRelation *pmdrel,
				CPartConstraint *ppartcnstrRel,
				DrgPcr *pdrgpcrOutput,
				CColRefSet *pcrsReqd,
				CColRefSet *pcrsScalarExpr,
				CColRefSet *pcrsAcceptedOuterRefs // set of columns to be considered for index apply
				);

			// compute the newly covered part constraint based on the old covered part
			// constraint and the given part constraint
			static
			CPartConstraint *PpartcnstrUpdateCovered
				(
				IMemoryPool *pmp,
				CMDAccessor *pmda,
				DrgPexpr *pdrgpexprScalar,
				CPartConstraint *ppartcnstrCovered,
				CPartConstraint *ppartcnstr,
				DrgPcr *pdrgpcrOutput,
				DrgPexpr *pdrgpexprIndex,
				DrgPexpr *pdrgpexprResidual,
				const IMDRelation *pmdrel,
				const IMDIndex *pmdindex,
				CColRefSet *pcrsAcceptedOuterRefs
				);

			// remap the expression from the old columns to the new ones
			static
			CExpression *PexprRemapColumns
				(
				IMemoryPool *pmp,
				CExpression *pexprScalar,
				DrgPcr *pdrgpcrA,
				DrgPcr *pdrgpcrRemappedA,
				DrgPcr *pdrgpcrB,
				DrgPcr *pdrgpcrRemappedB
				);

			// construct a partial dynamic index get
			static
			CExpression *PexprPartialDynamicIndexGet
				(
				IMemoryPool *pmp,
				CLogicalDynamicGet *popGet,
				ULONG ulOriginOpId,
				DrgPexpr *pdrgpexprIndex,
				DrgPexpr *pdrgpexprResidual,
				DrgPcr *pdrgpcrDIG,
				const IMDIndex *pmdindex,
				const IMDRelation *pmdrel,
				CPartConstraint *ppartcnstr,
				CColRefSet *pcrsAcceptedOuterRefs,  // set of columns to be considered for index apply
				DrgPcr *pdrgpcrOuter,
				DrgPcr *pdrgpcrNewOuter
				);

			// create a new CTE consumer for the given CTE id
			static
			CExpression *PexprCTEConsumer(IMemoryPool *pmp, ULONG ulCTEId, DrgPcr *pdrgpcrConsumer);

			// return a new array containing the columns from the given column array 'pdrgpcr'
			// at the positions indicated by the given ULONG array 'pdrgpulIndexesOfRefs'
			// e.g., pdrgpcr = {col1, col2, col3}, pdrgpulIndexesOfRefs = {2, 1}
			// the result will be {col3, col2}
			static
			DrgPcr *PdrgpcrReorderedSubsequence
				(
				IMemoryPool *pmp,
				DrgPcr *pdrgpcr,
				DrgPul *pdrgpulIndexesOfRefs
				);

			// check if given xform is an Agg splitting xform
			static
			BOOL FSplitAggXform(CXform::EXformId exfid);

			// check if given expression is a multi-stage Agg based on origin xform
			static
			BOOL FMultiStageAgg(CExpression *pexprAgg);

			// check if expression handle is attached to a Join with a predicate that uses columns from only one child
			static
			BOOL FJoinPredOnSingleChild(IMemoryPool *pmp, CExpressionHandle &exprhdl);

			// add a redundant SELECT node on top of Dynamic (Bitmap) IndexGet to be able to use index
			// predicate in partition elimination
			static
			CExpression *PexprRedundantSelectForDynamicIndex(IMemoryPool *pmp, CExpression *pexpr);

			// convert an Agg window function into regular Agg
			static
			CExpression *PexprWinFuncAgg2ScalarAgg(IMemoryPool *pmp, CExpression *pexprWinFunc);

			// create a map from the argument of each Distinct Agg to the array of project elements that define Distinct Aggs on the same argument
			static
			void MapPrjElemsWithDistinctAggs(IMemoryPool *pmp, CExpression *pexprPrjList, HMExprDrgPexpr **pphmexprdrgpexpr, ULONG *pulDifferentDQAs);

			// convert GbAgg with distinct aggregates to a join
			static
			CExpression *PexprGbAggOnCTEConsumer2Join(IMemoryPool *pmp, CExpression *pexprGbAgg);

	}; // class CXformUtils
}

#include "CXformUtils.inl"

#endif // !GPOPT_CXformUtils_H

// EOF
