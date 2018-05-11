//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 - 2011 EMC CORP.
//
//	@filename:
//		CExpression.h
//
//	@doc:
//		Basic tree/DAG-based representation for an expression
//---------------------------------------------------------------------------
#ifndef GPOPT_CExpression_H
#define GPOPT_CExpression_H

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"
#include "gpos/common/CDynamicPtrArray.h"

#include "naucrates/statistics/CStatistics.h"
#include "gpopt/cost/CCost.h"
#include "gpopt/base/CColRef.h"
#include "gpopt/base/CCostContext.h"
#include "gpopt/base/CDrvdPropRelational.h"
#include "gpopt/base/CDrvdPropScalar.h"
#include "gpopt/base/CReqdProp.h"
#include "gpopt/base/CReqdPropRelational.h"
#include "gpopt/base/CPrintPrefix.h"
#include "gpopt/operators/COperator.h"


namespace gpopt
{
	// cleanup function for arrays
	class CExpression;	
	typedef CDynamicPtrArray<CExpression, CleanupRelease> CExpressionArray;

	// array of arrays of expression pointers
	typedef CDynamicPtrArray<CExpressionArray, CleanupRelease> CExpressionArrays;

	class CGroupExpression;
	class CDrvdPropPlan;
	class CDrvdPropCtxt;
	class CDrvdPropCtxtPlan;

	using namespace gpos;
	using namespace gpnaucrates;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CExpression
	//
	//	@doc:
	//		Simply dynamic array for pointer types
	//
	//---------------------------------------------------------------------------
	class CExpression : public CRefCount
	{
		private:
		
			// memory pool
			IMemoryPool *m_mp;
			
			// operator class
			COperator *m_pop;
			
			// array of children
			CExpressionArray *m_pdrgpexpr;

			// derived relational properties
			CDrvdPropRelational *m_pdprel;

			// derived stats
			IStatistics *m_pstats;

			// required plan properties
			CReqdPropPlan *m_prpp;

			// derived physical properties
			CDrvdPropPlan *m_pdpplan;

			// derived scalar properties
			CDrvdPropScalar *m_pdpscalar;

			// group reference to Memo
			CGroupExpression *m_pgexpr;

			// cost of physical expression node when copied out of the memo
			CCost m_cost;

			// id of origin group, used for debugging expressions extracted from memo
			ULONG m_ulOriginGrpId;

			// id of origin group expression, used for debugging expressions extracted from memo
			ULONG m_ulOriginGrpExprId;

			// set expression's derivable property
			void SetPdp(DrvdPropArray *pdp, const DrvdPropArray::EPropType ept);

#ifdef GPOS_DEBUG

			// assert valid property derivation
			void AssertValidPropDerivation(const DrvdPropArray::EPropType ept);

			// print expression properties
			void PrintProperties(IOstream &os, CPrintPrefix &pfx) const;

#endif // GPOS_DEBUG

			// check if the expression satisfies partition enforcer condition
			BOOL FValidPartEnforcers(CDrvdPropCtxtPlan *pdpctxtplan);

			// check if the distributions of all children are compatible
			BOOL FValidChildrenDistribution(CDrvdPropCtxtPlan *pdpctxtplan);

			// copy group properties and stats to expression
			void CopyGroupPropsAndStats(IStatistics *input_stats);

			// decorate expression tree with required plan properties
			CReqdPropPlan* PrppDecorate(IMemoryPool *mp, CReqdPropPlan *prppInput);

			// private copy ctor
			CExpression(const CExpression &);
						
		public:
		
			// ctor's with different arity

			// ctor for leaf nodes
			CExpression
				(
				IMemoryPool *mp,
				COperator *pop,
				CGroupExpression *pgexpr = NULL
				);

			// ctor for unary expressions
			CExpression
				(
				IMemoryPool *mp,
				COperator *pop,
				CExpression *pexpr
				);

			// ctor for binary expressions
			CExpression
				(
				IMemoryPool *mp,
				COperator *pop,
				CExpression *pexprChildFirst,
				CExpression *pexprChildSecond
				);

			// ctor for ternary expressions
			CExpression
				(
				IMemoryPool *mp,
				COperator *pop,
				CExpression *pexprChildFirst,
				CExpression *pexprChildSecond,
				CExpression *pexprChildThird
				);

			// ctor n-ary expressions
			CExpression
				(
				IMemoryPool *mp,
				COperator *pop,
				CExpressionArray *pdrgpexpr
				);
			
			// ctor for n-ary expression with origin group expression
			CExpression
				(
				IMemoryPool *mp,
				COperator *pop,
				CGroupExpression *pgexpr,
				CExpressionArray *pdrgpexpr,
				IStatistics *input_stats,
				CCost cost = GPOPT_INVALID_COST
				);

			// ctor for expression with derived properties
			CExpression
				(
				IMemoryPool *mp,
				DrvdPropArray *pdprop
				);
			
			// dtor
			~CExpression();
			
			// shorthand to access children
			CExpression *operator [] 
				(
				ULONG ulPos
				)
			const
			{
				GPOS_ASSERT(NULL != m_pdrgpexpr);
				return (*m_pdrgpexpr)[ulPos];
			};
	
			// arity function
			ULONG Arity() const
			{
				return m_pdrgpexpr == NULL ? 0 : m_pdrgpexpr->Size();
			}
			
			// accessor for operator
			COperator *Pop() const
			{
				GPOS_ASSERT(NULL != m_pop);
				return m_pop;
			}
		
			// accessor of children array
			CExpressionArray *PdrgPexpr() const
			{
				return m_pdrgpexpr;
			}

			// accessor for origin group expression
			CGroupExpression *Pgexpr() const
			{
				return m_pgexpr;
			}

			// accessor for computed required plan props
			CReqdPropPlan *Prpp() const
			{
				return m_prpp;
			}

			// get expression's derived property given its type
			DrvdPropArray *Pdp(const DrvdPropArray::EPropType ept) const;

			// get derived statistics object
			const IStatistics *Pstats() const
			{
				return m_pstats;
			}

			// cost accessor
			CCost Cost() const
			{
				return m_cost;
			}

			// get the suitable derived property type based on operator
			DrvdPropArray::EPropType Ept() const;

			// derive properties, determine the suitable derived property type internally
			DrvdPropArray *PdpDerive(CDrvdPropCtxt *pdpctxt = NULL);

			// derive statistics
			IStatistics *PstatsDerive(CReqdPropRelational *prprel, IStatisticsArray *stats_ctxt);

			// reset a derived property
			void ResetDerivedProperty(DrvdPropArray::EPropType ept);

			// reset all derived properties
			void ResetDerivedProperties();

			// reset expression stats
			void ResetStats();

			// compute required plan properties of all expression nodes
			CReqdPropPlan* PrppCompute(IMemoryPool *mp, CReqdPropPlan *prppInput);

			// check for outer references
			BOOL HasOuterRefs();

			// print driver
			IOstream &OsPrint
						(
						IOstream &os,
						const CPrintPrefix * = NULL,
						BOOL fLast = true
						)
						const;
			
			// match with group expression
			BOOL FMatchPattern(CGroupExpression *pgexpr) const;
			
			// return a copy of the expression with remapped columns
			CExpression *PexprCopyWithRemappedColumns(IMemoryPool *mp, UlongToColRefMap *colref_mapping, BOOL must_exist) const;

			// compare entire expression rooted here
			BOOL Matches(CExpression *pexpr) const;

#ifdef GPOS_DEBUG
			// match against given pattern
			BOOL FMatchPattern(CExpression *pexpr) const;
			
			// match children against given pattern
			BOOL FMatchPatternChildren(CExpression *pexpr) const;
			
			// compare entire expression rooted here
			BOOL FMatchDebug(CExpression *pexpr) const;

			// debug print; for interactive debugging sessions only
			void DbgPrint() const;

			// debug print; for interactive debugging sessions only
			// prints expression properties as well
			void DbgPrintWithProperties() const;
#endif // GPOS_DEBUG

			// check if the expression satisfies given required properties
			BOOL FValidPlan(const CReqdPropPlan *prpp, CDrvdPropCtxtPlan *pdpctxtplan);

			// static hash function
			static
			ULONG HashValue(const CExpression *pexpr);

			// static hash function
			static
			ULONG UlHashDedup(const CExpression *pexpr);

			// rehydrate expression from a given cost context and child expressions
			static
			CExpression *PexprRehydrate(IMemoryPool *mp, CCostContext *pcc, CExpressionArray *pdrgpexpr, CDrvdPropCtxtPlan *pdpctxtplan);


	}; // class CExpression


	// shorthand for printing
	inline
	IOstream &operator << (IOstream &os, CExpression &expr)
	{
		return expr.OsPrint(os);
	}

	// hash map from ULONG to expression
	typedef CHashMap<ULONG, CExpression, gpos::HashValue<ULONG>, gpos::Equals<ULONG>,
					CleanupDelete<ULONG>, CleanupRelease<CExpression> > UlongToExprMap;

	// map iterator
	typedef CHashMapIter<ULONG, CExpression, gpos::HashValue<ULONG>, gpos::Equals<ULONG>,
					CleanupDelete<ULONG>, CleanupRelease<CExpression> > UlongToExprMapIter;

}


#endif // !GPOPT_CExpression_H

// EOF
