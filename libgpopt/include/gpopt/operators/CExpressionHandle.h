//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 - 2011 EMC CORP.
//
//	@filename:
//		CExpressionHandle.h
//
//	@doc:
//		Handle to convey context wherever an expression is used in a shallow 
//		context, i.e. operator and the properties of its children but no
//		access to the children is needed.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CExpressionHandle_H
#define GPOPT_CExpressionHandle_H

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"
#include "gpos/common/CDynamicPtrArray.h"

#include "naucrates/statistics/CStatistics.h"

#include "gpopt/base/CDrvdProp.h"
#include "gpopt/base/CReqdProp.h"
#include "gpopt/base/CDrvdPropRelational.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/search/CGroupExpression.h"


namespace gpopt
{
	// fwd declaration
	class COperator;	
	class CDrvdPropPlan;
	class CDrvdPropScalar;
	class CColRefSet;
	class CCostContext;

	using namespace gpos;
	


	//---------------------------------------------------------------------------
	//	@class:
	//		CExpressionHandle
	//
	//	@doc:
	//		Context for expression; abstraction for group expressions and 
	//		stand-alone expressions/DAGs;
	//		a handle is attached to either an expression or a group expression
	//
	//---------------------------------------------------------------------------
	class CExpressionHandle 
	{
		private:
					
			// memory pool
			IMemoryPool *m_pmp;
			
			// attached expression
			CExpression *m_pexpr;
			
			// attached group expression
			CGroupExpression *m_pgexpr;

			// attached cost context
			CCostContext *m_pcc;

			// derived properties of attached expr/gexpr;
			// set during derived property computation
			CDrvdProp *m_pdp;

			// statistics of attached expr/gexpr;
			// set during derived stats computation
			IStatistics *m_pstats;

			// required properties of attached expr/gexpr;
			// set during required property computation
			CReqdProp *m_prp;

			// array of children's derived properties
			DrgPdp *m_pdrgpdp;

			// array of children's derived stats
			DrgPstat *m_pdrgpstat;

			// array of children's required properties
			DrgPrp *m_pdrgprp;

			// private copy ctor
			CExpressionHandle(const CExpressionHandle &);

			// cache properties of group and its children on the handle
			void CopyGroupProps();

			// cache properties of expression and its children on the handle
			void CopyExprProps();

			// cache properties of cost context and its children on the handle
			void CopyCostCtxtProps();

			// derive the properties of the plan carried by attached cost context,
			// uses passed context object during derivation
			void DerivePlanProps(CDrvdPropCtxtPlan *pdpctxtplan);

			// return an array of stats objects starting from the first stats object referenced by child
			DrgPstat *PdrgpstatOuterRefs(DrgPstat *pdrgpstat, ULONG ulChildIndex) const;

			// check if stats are derived for attached expression and its children
			BOOL FStatsDerived() const;

			// copy stats from attached expression/group expression to local stats members
			void CopyStats();

			// return True if handle is attached to a leaf pattern
			BOOL FAttachedToLeafPattern() const;

			// stat derivation at root operator where handle is attached
			void DeriveRootStats(DrgPstat *pdrgpstatCtxt);

		public:
		
			// ctor
			explicit
			CExpressionHandle(IMemoryPool *pmp);

			// dtor
			~CExpressionHandle();

			// attach handle to a given expression
			void Attach(CExpression *pexpr);

			// attach handle to a given group expression
			void Attach(CGroupExpression *pgexpr);

			// attach handle to a given cost context
			void Attach(CCostContext *pcc);

			// recursive property derivation,
			void DeriveProps(CDrvdPropCtxt *pdpctxt);

			// recursive stats derivation
			void DeriveStats(DrgPstat *pdrgpstatCtxt, BOOL fComputeRootStats = true);

			// stats derivation for attached cost context
			void DeriveCostContextStats();

			// stats derivation using given properties and context
			void DeriveStats(IMemoryPool *pmpLocal, IMemoryPool *pmpGlobal, CReqdPropRelational *prprel, DrgPstat *pdrgpstatCtxt);

			// derive the properties of the plan carried by attached cost context
			void DerivePlanProps();

			// initialize required properties container
			void InitReqdProps(CReqdProp *prpInput);

			// compute required properties of the n-th child
			void ComputeChildReqdProps(ULONG ulChildIndex, DrgPdp *pdrgpdpCtxt, ULONG ulOptReq);

			// copy required properties of the n-th child
			void CopyChildReqdProps(ULONG ulChildIndex, CReqdProp *prp);

			// compute required columns of the n-th child
			void ComputeChildReqdCols(ULONG ulChildIndex, DrgPdp *pdrgpdpCtxt);

			// required properties computation of all children
			void ComputeReqdProps(CReqdProp *prpInput, ULONG ulOptReq);

			// derived relational props of n-th child
			CDrvdPropRelational *Pdprel(ULONG ulChildIndex) const;

			// derived stats of n-th child
			IStatistics *Pstats(ULONG ulChildIndex) const;

			// derived plan props of n-th child
			CDrvdPropPlan *Pdpplan(ULONG ulChildIndex) const;

			// derived scalar props of n-th child
			CDrvdPropScalar *Pdpscalar(ULONG ulChildIndex) const;

			// derived properties of attached expr/gexpr
			CDrvdProp *Pdp() const
			{
				return m_pdp;
			}

			// derived relational properties of attached expr/gexpr
			CDrvdPropRelational *Pdprel() const;

			// stats of attached expr/gexpr
			IStatistics *Pstats() const
			{
				return m_pstats;
			}

			// required properties of attached expr/gexpr
			CReqdProp *Prp() const
			{
				return m_prp;
			}

			// check if given child is a scalar
			BOOL FScalarChild(ULONG ulChildIndex) const;

			// required relational props of n-th child
			CReqdPropRelational *Prprel(ULONG ulChildIndex) const;

			// required plan props of n-th child
			CReqdPropPlan *Prpp(ULONG ulChildIndex) const;
	
			// arity function
			ULONG UlArity() const;

			// index of the last non-scalar child
			ULONG UlLastNonScalarChild() const;

			// index of the first non-scalar child
			ULONG UlFirstNonScalarChild() const;

			// number of non-scalar children
			ULONG UlNonScalarChildren() const;

			// accessor for operator
			COperator *Pop() const;

			// accessor for child operator
			COperator *Pop(ULONG ulChildIndex) const;

			// accessor for expression
			CExpression *Pexpr() const
			{
				return m_pexpr;
			}

			// accessor for group expression
			CGroupExpression *Pgexpr() const
			{
				return m_pgexpr;
			}

			// check for outer references
			BOOL FHasOuterRefs() const
			{
				return (0 < Pdprel()->PcrsOuter()->CElements());
			}

			// check if attached expression must execute on the master
			BOOL FMasterOnly() const
			{
				return Pdprel()->Pfp()->FMasterOnly();
			}

			// check for outer references in the given child
			BOOL FHasOuterRefs
				(
				ULONG ulChildIndex
				)
				const
			{
				return (0 < Pdprel(ulChildIndex)->PcrsOuter()->CElements());
			}

			// get next child index based on child optimization order, return true if such index could be found
			BOOL FNextChildIndex
				(
				ULONG *pulChildIndex // output: index to be changed
				)
				const;

			// return the index of first child to be optimized
			ULONG UlFirstOptimizedChildIndex() const;

			// return the index of last child to be optimized
			ULONG UlLastOptimizedChildIndex() const;

			// return the index of child to be optimized next to the given child
			ULONG UlNextOptimizedChildIndex(ULONG ulChildIndex) const;

			// return the index of child optimized before the given child
			ULONG UlPreviousOptimizedChildIndex(ULONG ulChildIndex) const;

			// get the function properties of a child
			CFunctionProp *PfpChild(ULONG ulChildIndex) const;

			// check whether an expression's children have a volatile function
			BOOL FChildrenHaveVolatileFuncScan() const;

			// return the scalar child at given index
			CExpression *PexprScalarChild(ULONG ulChildIndex) const;

			// return the scalar expression attached to handle
			CExpression *PexprScalar() const;

			void DeriveProducerStats(ULONG ulChildIndex, CColRefSet *pcrsStat);

			// return the columns used by a logical operator internally as well
			// as columns used by all its scalar children
			CColRefSet *PcrsUsedColumns(IMemoryPool *pmp);

	}; // class CExpressionHandle
	
}


#endif // !GPOPT_CExpressionHandle_H

// EOF
