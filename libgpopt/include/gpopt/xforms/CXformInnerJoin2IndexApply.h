//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal, Inc.
//
//	@filename:
//		CXformInnerJoin2IndexApply.h
//
//	@doc:
//		Transform Inner Join to Index Apply
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformInnerJoin2IndexApply_H
#define GPOPT_CXformInnerJoin2IndexApply_H

#include "gpos/base.h"
#include "gpopt/xforms/CXformExploration.h"
#include "gpopt/xforms/CXformUtils.h"

namespace gpopt
{
	using namespace gpos;

	// fwd declaration
	class CLogicalDynamicGet;

	//---------------------------------------------------------------------------
	//	@class:
	//		CXformInnerJoin2IndexApply
	//
	//	@doc:
	//		Transform Inner Join to Index Apply
	//
	//---------------------------------------------------------------------------
	class CXformInnerJoin2IndexApply : public CXformExploration
	{

		private:

			// private copy ctor
			CXformInnerJoin2IndexApply(const CXformInnerJoin2IndexApply &);

			// helper to add IndexApply expression to given xform results container
			// for homogeneous b-tree indexes
			static
			void CreateHomogeneousBtreeIndexApplyAlternatives
				(
				IMemoryPool *pmp,
				ULONG ulOriginOpId,
				CExpression *pexprOuter,
				CExpression *pexprInner,
				CExpression *pexprScalar,
				CTableDescriptor *ptabdescInner,
				CLogicalDynamicGet *popDynamicGet,
				CColRefSet *pcrsScalarExpr,
				CColRefSet *pcrsOuterRefs,
				CColRefSet *pcrsReqd,
				ULONG ulIndices,
				CXformResult *pxfres
				);

			// helper to add IndexApply expression to given xform results container
			// for homogeneous b-tree indexes
			static
			void CreateAlternativesForBtreeIndex
				(
				IMemoryPool *pmp,
				ULONG ulOriginOpId,
				CExpression *pexprOuter,
				CExpression *pexprInner,
				CMDAccessor *pmda,
				DrgPexpr *pdrgpexprConjuncts,
				CColRefSet *pcrsScalarExpr,
				CColRefSet *pcrsOuterRefs,
				CColRefSet *pcrsReqd,
				const IMDRelation *pmdrel,
				const IMDIndex *pmdindex,
				CPartConstraint *ppartcnstrIndex,
				CXformResult *pxfres
				);

			// helper to add IndexApply expression to given xform results container
			// for homogeneous bitmap indexes
			static
			void CreateHomogeneousBitmapIndexApplyAlternatives
				(
				IMemoryPool *pmp,
				ULONG ulOriginOpId,
				CExpression *pexprOuter,
				CExpression *pexprInner,
				CExpression *pexprScalar,
				CTableDescriptor *ptabdescInner,
				CColRefSet *pcrsOuterRefs,
				CColRefSet *pcrsReqd,
				CXformResult *pxfres
				);

			// based on the inner and the scalar expression, it computes scalar expression
			// columns, outer references and required columns
			static
			void ComputeColumnSets
				(
				IMemoryPool *pmp,
				CExpression *pexprInner,
				CExpression *pexprScalar,
				CColRefSet **ppcrsScalarExpr,
				CColRefSet **ppcrsOuterRefs,
				CColRefSet **ppcrsReqd
				);

			// create an index apply plan when applicable
			static
			void CreatePartialIndexApplyPlan
					(
					IMemoryPool *pmp,
					ULONG ulOriginOpId,
					CExpression *pexprOuter,
					CExpression *pexprScalar,
					CColRefSet *pcrsOuterRefs,
					CLogicalDynamicGet *popDynamicGet,
					DrgPpartdig *pdrgppartdig,
					const IMDRelation *pmdrel,
					CXformResult *pxfres
					);

			// create an inner join with a CTE consumer on the inner branch, with the given
			// partition constraint
			static
			CExpression *PexprInnerJoinOverCTEConsumer
				(
				IMemoryPool *pmp,
				ULONG ulOriginOpId,
				CLogicalDynamicGet *popDynamicGet,
				ULONG ulCTEId,
				CExpression *pexprScalar,
				DrgPcr *pdrgpcrDynamicGet,
				CPartConstraint *ppartcnstr,
				DrgPcr *pdrgpcrOuter,
				DrgPcr *pdrgpcrOuterNew
				);

			// create an index apply with a CTE consumer on the outer branch
			// and a dynamic get on the inner one
			static
			CExpression *PexprIndexApplyOverCTEConsumer
				(
				IMemoryPool *pmp,
				ULONG ulOriginOpId,
				CLogicalDynamicGet *popDynamicGet,
				DrgPexpr *pdrgpexprIndex,
				DrgPexpr *pdrgpexprResidual,
				DrgPcr *pdrgpcrIndexGet,
				const IMDIndex *pmdindex,
				const IMDRelation *pmdrel,
				BOOL fFirst,
				ULONG ulCTEId,
				CPartConstraint *ppartcnstr,
				CColRefSet *pcrsOuterRefs,
				DrgPcr *pdrgpcrOuter,
				DrgPcr *pdrgpcrOuterNew,
				DrgPcr *pdrgpcrOuterRefsInScan,
				DrgPul *pdrgpulIndexesOfRefsInScan
				);

			// create a union-all with the given children
			static
			CExpression *PexprConstructUnionAll
				(
				IMemoryPool *pmp,
				DrgPcr *pdrgpcrLeftSchema,
				DrgPcr *pdrgpcrRightSchema,
				CExpression *pexprLeftChild,
				CExpression *pexprRightChild,
				ULONG ulScanId
				);

			//	construct a CTE Anchor over the given UnionAll and adds it to the given
			//	Xform result
			static
			void AddUnionPlanForPartialIndexes
				(
				IMemoryPool *pmp,
				CLogicalDynamicGet *popDynamicGet,
				ULONG ulCTEId,
				CExpression *pexprUnion,
				CExpression *pexprScalar,
				CXformResult *pxfres
				);

		protected:
			// helper to add IndexApply expression to given xform results container
			// for homogeneous indexes
			static
			void CreateHomogeneousIndexApplyAlternatives
				(
				IMemoryPool *pmp,
				ULONG ulOriginOpId,
				CExpression *pexprOuter,
				CExpression *pexprInner,
				CExpression *pexprScalar,
				CTableDescriptor *PtabdescInner,
				CLogicalDynamicGet *popDynamicGet,
				CXformResult *pxfres,
				gpmd::IMDIndex::EmdindexType emdtype
				);

			// helper to add IndexApply expression to given xform results container
			// for partial indexes
			static
			void CreatePartialIndexApplyAlternatives
				(
				IMemoryPool *pmp,
				ULONG ulOriginOpId,
				CExpression *pexprOuter,
				CExpression *pexprInner,
				CExpression *pexprScalar,
				CTableDescriptor *PtabdescInner,
				CLogicalDynamicGet *popDynamicGet,
				CXformResult *pxfres
				);

		public:

			// ctor
			explicit
			CXformInnerJoin2IndexApply
				(
				CExpression *pexprPattern
				)
				:
				CXformExploration(pexprPattern)
			{}

			// dtor
			virtual
			~CXformInnerJoin2IndexApply()
			{}

			// compute xform promise for a given expression handle
			virtual
			EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	}; // class CXformInnerJoin2IndexApply

}

#endif // !GPOPT_CXformInnerJoin2IndexApply_H

// EOF
