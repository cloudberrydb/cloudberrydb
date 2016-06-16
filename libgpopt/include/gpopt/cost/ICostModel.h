//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal Inc.
//
//	@filename:
//		ICostModel.h
//
//	@doc:
//		Interface for the underlying cost model
//---------------------------------------------------------------------------



#ifndef GPOPT_ICostModel_H
#define GPOPT_ICostModel_H

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"

#include "naucrates/statistics/IStatistics.h"

#include "gpopt/operators/COperator.h"
#include "naucrates/md/IMDRelation.h"
#include "CCost.h"
#include "ICostModelParams.h"

// default number of rebinds (number of times a plan is executed due to rebinding to external parameters)
#define GPOPT_DEFAULT_REBINDS 1

namespace gpopt
{
	// fwd declarations
	class CExpressionHandle;

	using namespace gpos;
	using namespace gpmd;
	using namespace gpnaucrates;

	// dynamic array of cost model params
	typedef CDynamicPtrArray<ICostModelParams::SCostParam, CleanupDelete> DrgPcp;

	//---------------------------------------------------------------------------
	//	@class:
	//		ICostModel
	//
	//	@doc:
	//		Interface for the underlying cost model
	//
	//---------------------------------------------------------------------------
	class ICostModel : public CRefCount
	{
		public:

			enum
			ECostModelType
			{
				EcmtGPDBLegacy = 0,
				EcmtGPDBCalibrated = 1,
				EcmtSentinel = 2
			};
			
			//---------------------------------------------------------------------------
			//	@class:
			//		SCostingStas
			//
			//	@doc:
			//		Stast information used during cost computation
			//
			//---------------------------------------------------------------------------
			class CCostingStats : public CRefCount
			{
				private:
					// stats of the root
					IStatistics *m_pstats;

				public:
					// ctor
					CCostingStats
						(
						IStatistics *pstats
						)
						:
						m_pstats(pstats)
					{
						GPOS_ASSERT(NULL != pstats);
					}

					// dtor
					~CCostingStats()
					{
						m_pstats->Release();
					}

					// the risk of errors in cardinality estimation
					ULONG UlStatsEstimationRisk() const
					{
						return m_pstats->UlStatsEstimationRisk();
					}

					// look up the number of distinct values of a particular column
					CDouble DNDV(const CColRef *pcr)
					{
						return m_pstats->DNDV(pcr);
					}
			};  // class CCostingStats

			//---------------------------------------------------------------------------
			//	@class:
			//		SCostingInfo
			//
			//	@doc:
			//		Information used during cost computation
			//
			//---------------------------------------------------------------------------
			struct SCostingInfo
			{

				private:

					// number of children excluding scalar children
					ULONG m_ulChildren;

					// stats of the root
					CCostingStats *m_pcstats;

					// row estimate of root
					DOUBLE m_dRows;

					// width estimate of root
					DOUBLE m_dWidth;

					// number of rebinds of root
					DOUBLE m_dRebinds;

					// row estimates of child operators
					DOUBLE *m_pdRowsChildren;

					// width estimates of child operators
					DOUBLE *m_pdWidthChildren;

					 // number of rebinds of child operators
					DOUBLE *m_pdRebindsChildren;

					// computed cost of child operators
					DOUBLE *m_pdCostChildren;

				public:

					// ctor
					SCostingInfo
						(
						ULONG ulChildren,
						CCostingStats *pcstats,
						DOUBLE dRows,
						DOUBLE dWidth,
						DOUBLE dRebinds,
						DOUBLE *pdRowsChildren,
						DOUBLE *pdWidthChildren,
						DOUBLE *pdRebindsChildren,
						DOUBLE *pdCostChildren
						)
						:
						m_ulChildren(ulChildren),
						m_pcstats(pcstats),
						m_dRows(dRows),
						m_dWidth(dWidth),
						m_dRebinds(dRebinds),
						m_pdRowsChildren(pdRowsChildren),
						m_pdWidthChildren(pdWidthChildren),
						m_pdRebindsChildren(pdRebindsChildren),
						m_pdCostChildren(pdCostChildren)
					{
						GPOS_ASSERT(NULL != pcstats);
					};

					// ctor
					SCostingInfo
						(
						IMemoryPool *pmp,
						ULONG ulChildren,
						CCostingStats *pcstats
						)
						:
						m_ulChildren(ulChildren),
						m_pcstats(pcstats),
						m_dRows(0),
						m_dWidth(0),
						m_dRebinds(GPOPT_DEFAULT_REBINDS),
						m_pdRowsChildren(NULL),
						m_pdWidthChildren(NULL),
						m_pdRebindsChildren(NULL),
						m_pdCostChildren(NULL)
					{
						GPOS_ASSERT(NULL != pcstats);
						if (0 < ulChildren)
						{
							m_pdRowsChildren = GPOS_NEW_ARRAY(pmp, DOUBLE, ulChildren);
							m_pdWidthChildren = GPOS_NEW_ARRAY(pmp, DOUBLE, ulChildren);
							m_pdRebindsChildren = GPOS_NEW_ARRAY(pmp, DOUBLE, ulChildren);
							m_pdCostChildren = GPOS_NEW_ARRAY(pmp, DOUBLE, ulChildren);
						}
					}

					// dtor
					~SCostingInfo()
					{
						GPOS_DELETE_ARRAY(m_pdRowsChildren);
						GPOS_DELETE_ARRAY(m_pdWidthChildren);
						GPOS_DELETE_ARRAY(m_pdRebindsChildren);
						GPOS_DELETE_ARRAY(m_pdCostChildren);
						m_pcstats->Release();
					}

					// children accessor
					ULONG UlChildren() const
					{
						return m_ulChildren;
					}

					// rows accessor
					DOUBLE DRows() const
					{
						return m_dRows;
					}

					// rows setter
					void SetRows
						(
						DOUBLE dRows
						)
					{
						GPOS_ASSERT(0 <= dRows);

						m_dRows = dRows;
					}

					// width accessor
					DOUBLE DWidth() const
					{
						return m_dWidth;
					}

					// width setter
					void SetWidth
						(
						DOUBLE dWidth
						)
					{
						GPOS_ASSERT(0 <= dWidth);

						m_dWidth = dWidth;
					}

					// rebinds accessor
					DOUBLE DRebinds() const
					{
						return m_dRebinds;
					}

					// rebinds setter
					void SetRebinds
						(
						DOUBLE dRebinds
						)
					{
						GPOS_ASSERT(GPOPT_DEFAULT_REBINDS <= dRebinds);

						m_dRebinds = dRebinds;
					}

					// children rows accessor
					DOUBLE *PdRows() const
					{
						return m_pdRowsChildren;
					}

					// child rows setter
					void SetChildRows
						(
						ULONG ulPos,
						DOUBLE dRowsChild
						)
					{
						GPOS_ASSERT(0 <= dRowsChild);
						GPOS_ASSERT(ulPos < m_ulChildren);

						m_pdRowsChildren[ulPos] = dRowsChild;
					}

					// children width accessor
					DOUBLE *PdWidth() const
					{
						return m_pdWidthChildren;
					}

					// child width setter
					void SetChildWidth
						(
						ULONG ulPos,
						DOUBLE dWidthChild
						)
					{
						GPOS_ASSERT(0 <= dWidthChild);
						GPOS_ASSERT(ulPos < m_ulChildren);

						m_pdWidthChildren[ulPos] = dWidthChild;
					}

					// children rebinds accessor
					DOUBLE *PdRebinds() const
					{
						return m_pdRebindsChildren;
					}

					// child rebinds setter
					void SetChildRebinds
						(
						ULONG ulPos,
						DOUBLE dRebindsChild
						)
					{
						GPOS_ASSERT(GPOPT_DEFAULT_REBINDS <= dRebindsChild);
						GPOS_ASSERT(ulPos < m_ulChildren);

						m_pdRebindsChildren[ulPos] = dRebindsChild;
					}

					// children cost accessor
					DOUBLE *PdCost() const
					{
						return m_pdCostChildren;
					}

					// child cost setter
					void SetChildCost
						(
						ULONG ulPos,
						DOUBLE dCostChild
						)
					{
						GPOS_ASSERT(0 <= dCostChild);
						GPOS_ASSERT(ulPos < m_ulChildren);

						m_pdCostChildren[ulPos] = dCostChild;
					}

					// return additional cost statistics
					CCostingStats *Pcstats() const
					{
						return m_pcstats;
					}

			}; // struct SCostingInfo

			// return number of hosts (nodes) that store data
			virtual
			ULONG UlHosts() const = 0;

			// return number of rows per host
			virtual
			CDouble DRowsPerHost(CDouble dRowsTotal) const = 0;

			// return cost model parameters
			virtual
			ICostModelParams *Pcp() const = 0;

			// main driver for cost computation
			virtual
			CCost Cost(CExpressionHandle &exprhdl, const SCostingInfo *pci) const = 0;
			
			// cost model type
			virtual
			ECostModelType Ecmt() const = 0;
			
			// set cost model params
			void SetParams(DrgPcp *pdrgpcp);

			// create a default cost model instance
			static
			ICostModel *PcmDefault(IMemoryPool *pmp);
	};
}

#endif // !GPOPT_ICostModel_H

// EOF
