//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CCostModelGPDBLegacy.h
//
//	@doc:
//		GPDB's legacy (uncalibrated) cost model
//---------------------------------------------------------------------------
#ifndef GPDBCOST_CCostModelGPDBLegacy_H
#define GPDBCOST_CCostModelGPDBLegacy_H

#include "gpos/base.h"
#include "gpos/common/CDouble.h"

#include "gpopt/cost/CCost.h"
#include "gpopt/cost/ICostModel.h"
#include "gpopt/cost/ICostModelParams.h"

#include "gpdbcost/CCostModelParamsGPDBLegacy.h"


namespace gpdbcost
{
	using namespace gpos;
	using namespace gpopt;
	using namespace gpmd;


	//---------------------------------------------------------------------------
	//	@class:
	//		CCostModelGPDBLegacy
	//
	//	@doc:
	//		GPDB's legacy cost model
	//
	//---------------------------------------------------------------------------
	class CCostModelGPDBLegacy : public ICostModel
	{

		private:

			// definition of operator processor
			typedef CCost(FnCost)(IMemoryPool *, CExpressionHandle &, const CCostModelGPDBLegacy *, const SCostingInfo *);

			//---------------------------------------------------------------------------
			//	@struct:
			//		SCostMapping
			//
			//	@doc:
			//		Mapping of operator to a cost function
			//
			//---------------------------------------------------------------------------
			struct SCostMapping
			{
				// physical operator id
				COperator::EOperatorId m_eopid;

				// pointer to cost function
				FnCost *m_pfnc;

			}; // struct SCostMapping

			// memory pool
			IMemoryPool *m_pmp;

			// number of segments
			ULONG m_ulSegments;

			// cost model parameters
			CCostModelParamsGPDBLegacy *m_pcp;

			// array of mappings
			static
			const SCostMapping m_rgcm[];

			// return cost of processing the given number of rows
			static
			CCost CostTupleProcessing(DOUBLE dRows, DOUBLE dWidth, ICostModelParams *pcp);

			// helper function to return cost of a plan rooted by unary operator
			static
			CCost CostUnary(DOUBLE dRows, DOUBLE dWidth, DOUBLE dRebinds, DOUBLE *pdcostChildren, ICostModelParams *pcp);

			// cost of spooling
			static
			CCost CostSpooling(DOUBLE dRows, DOUBLE dWidth, DOUBLE dRebinds, DOUBLE *pdcostChildren, ICostModelParams *pcp);

			// cost of redistribute motion operators
			static
			CCost CostRedistribute(DOUBLE dRows, DOUBLE dWidth, ICostModelParams *pcp);

			// add up array of costs
			static
			CCost CostSum(DOUBLE *pdCost, ULONG ulSize);

			// check if given operator is unary
			static
			BOOL FUnary(COperator::EOperatorId eopid);

			// cost of scan
			static
			CCost CostScan(IMemoryPool *pmp, CExpressionHandle &exprhdl, const CCostModelGPDBLegacy *pcmgpdb, const SCostingInfo *pci);

			// cost of index scan
			static
			CCost CostIndexScan(IMemoryPool *pmp, CExpressionHandle &exprhdl, const CCostModelGPDBLegacy *pcmgpdb, const SCostingInfo *pci);

			// cost of bitmap table scan
			static
			CCost CostBitmapTableScan(IMemoryPool *pmp, CExpressionHandle &exprhdl, const CCostModelGPDBLegacy *pcmgpdb, const SCostingInfo *pci);

			// cost of sequence project
			static
			CCost CostSequenceProject(IMemoryPool *pmp, CExpressionHandle &exprhdl, const CCostModelGPDBLegacy *pcmgpdb, const SCostingInfo *pci);

			// cost of CTE producer
			static
			CCost CostCTEProducer(IMemoryPool *pmp, CExpressionHandle &exprhdl, const CCostModelGPDBLegacy *pcmgpdb, const SCostingInfo *pci);

			// cost of CTE consumer
			static
			CCost CostCTEConsumer(IMemoryPool *pmp, CExpressionHandle &exprhdl, const CCostModelGPDBLegacy *pcmgpdb, const SCostingInfo *pci);

			// cost of const table get
			static
			CCost CostConstTableGet(IMemoryPool *pmp, CExpressionHandle &exprhdl, const CCostModelGPDBLegacy *pcmgpdb, const SCostingInfo *pci);

			// cost of DML
			static
			CCost CostDML(IMemoryPool *pmp, CExpressionHandle &exprhdl, const CCostModelGPDBLegacy *pcmgpdb, const SCostingInfo *pci);

			// cost of hash agg
			static
			CCost CostHashAgg(IMemoryPool *pmp, CExpressionHandle &exprhdl, const CCostModelGPDBLegacy *pcmgpdb, const SCostingInfo *pci);

			// cost of scalar agg
			static
			CCost CostScalarAgg(IMemoryPool *pmp, CExpressionHandle &exprhdl, const CCostModelGPDBLegacy *pcmgpdb, const SCostingInfo *pci);

			// cost of stream agg
			static
			CCost CostStreamAgg(IMemoryPool *pmp, CExpressionHandle &exprhdl, const CCostModelGPDBLegacy *pcmgpdb, const SCostingInfo *pci);

			// cost of sequence
			static
			CCost CostSequence(IMemoryPool *pmp, CExpressionHandle &exprhdl, const CCostModelGPDBLegacy *pcmgpdb, const SCostingInfo *pci);

			// cost of sort
			static
			CCost CostSort(IMemoryPool *pmp, CExpressionHandle &exprhdl, const CCostModelGPDBLegacy *pcmgpdb, const SCostingInfo *pci);

			// cost of TVF
			static
			CCost CostTVF(IMemoryPool *pmp, CExpressionHandle &exprhdl, const CCostModelGPDBLegacy *pcmgpdb, const SCostingInfo *pci);

			// cost of UnionAll
			static
			CCost CostUnionAll(IMemoryPool *pmp, CExpressionHandle &exprhdl, const CCostModelGPDBLegacy *pcmgpdb, const SCostingInfo *pci);

			// cost of hash join
			static
			CCost CostHashJoin(IMemoryPool *pmp, CExpressionHandle &exprhdl, const CCostModelGPDBLegacy *pcmgpdb, const SCostingInfo *pci);

			// cost of nljoin
			static
			CCost CostNLJoin(IMemoryPool *pmp, CExpressionHandle &exprhdl, const CCostModelGPDBLegacy *pcmgpdb, const SCostingInfo *pci);

			// cost of inner or outer index-nljoin
			static
			CCost CostIndexNLJoin(IMemoryPool *pmp, CExpressionHandle &exprhdl, const CCostModelGPDBLegacy *pcmgpdb, const SCostingInfo *pci);

			// cost of motion
			static
			CCost CostMotion(IMemoryPool *pmp, CExpressionHandle &exprhdl, const CCostModelGPDBLegacy *pcmgpdb, const SCostingInfo *pci);

		public:

			// ctor
			CCostModelGPDBLegacy(IMemoryPool *pmp, ULONG ulSegments, DrgPcp *pdrgpcp = NULL);

			// dtor
			virtual
			~CCostModelGPDBLegacy();

			// number of segments
			ULONG UlHosts() const
			{
				return m_ulSegments;
			}

			// return number of rows per host
			virtual
			CDouble DRowsPerHost(CDouble dRowsTotal) const;

			// return cost model parameters
			virtual
			ICostModelParams *Pcp() const
			{
				return m_pcp;
			}
			
			// main driver for cost computation
			virtual
			CCost Cost(CExpressionHandle &exprhdl, const SCostingInfo *pci) const;
			
			// cost model type
			virtual
			ECostModelType Ecmt() const
			{
				return ICostModel::EcmtGPDBLegacy;
			}

	}; // class CCostModelGPDBLegacy

}

#endif // !GPDBCOST_CCostModelGPDBLegacy_H

// EOF
