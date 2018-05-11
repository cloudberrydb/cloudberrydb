//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		COptimizer.h
//
//	@doc:
//		Optimizer class, entry point for query optimization
//---------------------------------------------------------------------------
#ifndef GPOPT_COptimizer_H
#define GPOPT_COptimizer_H

#include "gpos/base.h"

#include "gpopt/eval/CConstExprEvaluatorDefault.h"
#include "gpopt/search/CSearchStage.h"
#include "naucrates/dxl/operators/CDXLNode.h"

namespace gpdxl
{
	class CDXLNode;
}

namespace gpmd
{
	class IMDProvider;
}


using namespace gpos;
using namespace gpdxl;

namespace gpopt
{

	// forward declarations
	class ICostModel;
	class COptimizerConfig;
	class CQueryContext;
	class CEnumeratorConfig;

	//---------------------------------------------------------------------------
	//	@class:
	//		COptimizer
	//
	//	@doc:
	//		Optimizer class, entry point for query optimization
	//
	//---------------------------------------------------------------------------
	class COptimizer
	{
		private:
			
			// handle exception after finalizing minidump
			static
			void HandleExceptionAfterFinalizingMinidump(CException &ex);

			// optimize query in the given query context
			static
			CExpression *PexprOptimize
				(
				IMemoryPool *pmp,
				CQueryContext *pqc,
				DrgPss *pdrgpss
				);

			// translate an optimizer expression into a DXL tree 
			static
			CDXLNode *Pdxln
						(
						IMemoryPool *pmp,
						CMDAccessor *pmda,
						CExpression *pexpr,
						DrgPcr *pdrgpcr,
						DrgPmdname *pdrgpmdname,
						ULONG ulHosts
						);

			// helper function to print query expression
			static
			void PrintQuery(IMemoryPool *pmp, CExpression *pexprTranslated, CQueryContext *pqc);

			// helper function to print query plan
			static
			void PrintPlan(IMemoryPool *pmp, CExpression *pexprPlan);

			// helper function to dump plan samples
			static
			void DumpSamples(IMemoryPool *pmp, CEnumeratorConfig *pec, ULONG ulSessionId, ULONG ulCmdId);

			// print query or plan tree
			static
			void PrintQueryOrPlan(IMemoryPool *pmp, CExpression *pexpr, CQueryContext *pqc = NULL);

			// Check for a plan with CTE, if both CTEProducer and CTEConsumer are executed on the same locality.
			static
			void CheckCTEConsistency(IMemoryPool *pmp, CExpression *pexpr);
		public:
			
			// main optimizer function 
			static
			CDXLNode *PdxlnOptimize
						(
						IMemoryPool *pmp, 
						CMDAccessor *pmda,						// MD accessor
						const CDXLNode *pdxlnQuery,
						const DrgPdxln *pdrgpdxlnQueryOutput, 	// required output columns
						const DrgPdxln *pdrgpdxlnCTE,
						IConstExprEvaluator *pceeval,			// constant expression evaluator
						ULONG ulHosts,							// number of hosts (data nodes) in the system
						ULONG ulSessionId,						// session id used for logging and minidumps
						ULONG ulCmdId,							// command id used for logging and minidumps
						DrgPss *pdrgpss,						// search strategy
						COptimizerConfig *poconf,				// optimizer configurations
						const CHAR *szMinidumpFileName = NULL	// name of minidump file to be created
						);
	}; // class COptimizer
}

#endif // !GPOPT_COptimizer_H

// EOF

