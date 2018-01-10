//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal, Inc.
//
//	Transform Inner/Outer Join to Index Apply
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformJoin2IndexApply_H
#define GPOPT_CXformJoin2IndexApply_H

#include "gpos/base.h"
#include "gpopt/xforms/CXformExploration.h"
#include "gpopt/xforms/CXformUtils.h"

namespace gpopt
{
	using namespace gpos;

	// fwd declaration
	class CLogicalDynamicGet;

	class CXformJoin2IndexApply : public CXformExploration
	{

		private:

			// private copy ctor
			CXformJoin2IndexApply(const CXformJoin2IndexApply &);

			// helper to add IndexApply expression to given xform results container
			// for homogeneous b-tree indexes
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
				) const;

			// helper to add IndexApply expression to given xform results container
			// for homogeneous b-tree indexes
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
				) const;

			// helper to add IndexApply expression to given xform results container
			// for homogeneous bitmap indexes
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
				) const;

			// based on the inner and the scalar expression, it computes scalar expression
			// columns, outer references and required columns
			void ComputeColumnSets
				(
				IMemoryPool *pmp,
				CExpression *pexprInner,
				CExpression *pexprScalar,
				CColRefSet **ppcrsScalarExpr,
				CColRefSet **ppcrsOuterRefs,
				CColRefSet **ppcrsReqd
				) const;

			// create an index apply plan when applicable
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
					) const;

			// create an join with a CTE consumer on the inner branch, with the given
			// partition constraint
			CExpression *PexprJoinOverCTEConsumer
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
				) const;

			// create an index apply with a CTE consumer on the outer branch
			// and a dynamic get on the inner one
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
				) const;

			// create a union-all with the given children
			CExpression *PexprConstructUnionAll
				(
				IMemoryPool *pmp,
				DrgPcr *pdrgpcrLeftSchema,
				DrgPcr *pdrgpcrRightSchema,
				CExpression *pexprLeftChild,
				CExpression *pexprRightChild,
				ULONG ulScanId
				) const;

			//	construct a CTE Anchor over the given UnionAll and adds it to the given
			//	Xform result
			void AddUnionPlanForPartialIndexes
				(
				IMemoryPool *pmp,
				CLogicalDynamicGet *popDynamicGet,
				ULONG ulCTEId,
				CExpression *pexprUnion,
				CExpression *pexprScalar,
				CXformResult *pxfres
				) const;

		protected:
			// helper to add IndexApply expression to given xform results container
			// for homogeneous indexes
			virtual
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
				) const;

			// helper to add IndexApply expression to given xform results container
			// for partial indexes
			virtual
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
				) const;

			virtual
			CLogicalJoin *PopLogicalJoin(IMemoryPool *pmp) const = 0;

			virtual
			CLogicalApply *PopLogicalApply
				(
				IMemoryPool *pmp,
				DrgPcr *pdrgpcrOuterRefs
				) const = 0;

		public:

			// ctor
			explicit
			CXformJoin2IndexApply
				(
				CExpression *pexprPattern
				)
				:
				CXformExploration(pexprPattern)
			{}

			// dtor
			virtual
			~CXformJoin2IndexApply()
			{}

			// compute xform promise for a given expression handle
			virtual
			EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	}; // class CXformJoin2IndexApply

}

#endif // !GPOPT_CXformJoin2IndexApply_H

// EOF
