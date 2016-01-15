//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		COptimizer.cpp
//
//	@doc:
//		Optimizer class implementation
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/common/CBitSet.h"
#include "gpos/error/CErrorHandlerStandard.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/md/IMDProvider.h"

#include "naucrates/traceflags/traceflags.h"
#include "gpopt/base/CAutoOptCtxt.h"
#include "gpopt/base/CQueryContext.h"
#include "gpopt/engine/CEngine.h"
#include "gpopt/engine/CEnumeratorConfig.h"
#include "gpopt/engine/CStatisticsConfig.h"
#include "gpopt/minidump/CMiniDumperDXL.h"
#include "gpopt/minidump/CMinidumperUtils.h"
#include "gpopt/minidump/CSerializableStackTrace.h"
#include "gpopt/minidump/CSerializableQuery.h"
#include "gpopt/minidump/CSerializablePlan.h"
#include "gpopt/minidump/CSerializableOptimizerConfig.h"
#include "gpopt/minidump/CSerializableMDAccessor.h"
#include "gpopt/mdcache/CMDAccessor.h"
#include "gpopt/translate/CTranslatorDXLToExpr.h"
#include "gpopt/translate/CTranslatorExprToDXL.h"

#include "gpopt/optimizer/COptimizerConfig.h"
#include "gpopt/optimizer/COptimizer.h"
#include "gpopt/cost/ICostModel.h"


using namespace gpos;
using namespace gpdxl;
using namespace gpmd;
using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		COptimizer::PrintQuery
//
//	@doc:
//		Helper function to print query expression
//
//---------------------------------------------------------------------------
void
COptimizer::PrintQuery
	(
	IMemoryPool *pmp,
	CExpression *pexprTranslated,
	CQueryContext *pqc
	)
{
	CAutoTrace at(pmp);
	at.Os() << std::endl << "Algebrized query: " << std::endl << *pexprTranslated;

	DrgPexpr *pdrgpexpr = COptCtxt::PoctxtFromTLS()->Pcteinfo()->PdrgPexpr(pmp);
	const ULONG ulCTEs = pdrgpexpr->UlLength();
	if (0 < ulCTEs)
	{
		at.Os() << std::endl << "Common Table Expressions: ";
		for (ULONG ul = 0; ul < ulCTEs; ul++)
		{
			at.Os() << std::endl << *(*pdrgpexpr)[ul];
		}
	}
	pdrgpexpr->Release();

	CExpression *pexprPreprocessed = pqc->Pexpr();
	(void) pexprPreprocessed->PdpDerive();
	at.Os() << std::endl << "Algebrized preprocessed query: " << std::endl << *pexprPreprocessed;
}


//---------------------------------------------------------------------------
//	@function:
//		COptimizer::PrintPlan
//
//	@doc:
//		Helper function to print query plan
//
//---------------------------------------------------------------------------
void
COptimizer::PrintPlan
	(
	IMemoryPool *pmp,
	CExpression *pexprPlan
	)
{
	CAutoTrace at(pmp);
	at.Os() << std::endl << "Physical plan: " << std::endl << *pexprPlan;
}


//---------------------------------------------------------------------------
//	@function:
//		COptimizer::DumpSamples
//
//	@doc:
//		Helper function to dump plan samples
//
//---------------------------------------------------------------------------
void
COptimizer::DumpSamples
	(
	IMemoryPool *pmp,
	CEnumeratorConfig *pec,
	ULONG ulSessionId,
	ULONG ulCmdId
	)
{
	GPOS_ASSERT(NULL != pec);

	CWStringDynamic *pstr = CDXLUtils::PstrSerializeSamplePlans(pmp, pec, true /*fIndent*/);
	pec->DumpSamples(pstr, ulSessionId, ulCmdId);
	GPOS_DELETE(pstr);
	GPOS_CHECK_ABORT;

	pec->FitCostDistribution();
	pstr = CDXLUtils::PstrSerializeCostDistr(pmp, pec, true /*fIndent*/);
	pec->DumpCostDistr(pstr, ulSessionId, ulCmdId);
	GPOS_DELETE(pstr);
}

//---------------------------------------------------------------------------
//	@function:
//		COptimizer::DumpQueryOrPlan
//
//	@doc:
//		Print query tree or plan tree
//
//---------------------------------------------------------------------------
void
COptimizer::PrintQueryOrPlan
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	CQueryContext *pqc
	)
{
	GPOS_ASSERT(NULL != pexpr);

	if (NULL != pqc)
	{
		if (GPOS_FTRACE(EopttracePrintQuery))
		{
			PrintQuery(pmp, pexpr, pqc);
		}
	}
	else
	{
		if (GPOS_FTRACE(EopttracePrintPlan))
		{
			PrintPlan(pmp, pexpr);
		}
	}
}


//---------------------------------------------------------------------------
//	@function:
//		COptimizer::PdxlnOptimize
//
//	@doc:
//		Optimize given query
//		the function is oblivious of trace flags setting/resetting which
//		must happen at the caller side if needed
//
//---------------------------------------------------------------------------
CDXLNode *
COptimizer::PdxlnOptimize
	(
	IMemoryPool *pmp, 
	CMDAccessor *pmda,
	const CDXLNode *pdxlnQuery,
	const DrgPdxln *pdrgpdxlnQueryOutput, 
	const DrgPdxln *pdrgpdxlnCTE, 
	IConstExprEvaluator *pceeval,
	ULONG ulHosts,	// actual number of data nodes in the system
	ULONG ulSessionId,
	ULONG ulCmdId,
	DrgPss *pdrgpss,
	COptimizerConfig *poconf,
	const CHAR *szMinidumpFileName 	// name of minidump file to be created
	)
{
	GPOS_ASSERT(NULL != pmda);
	GPOS_ASSERT(NULL != pdxlnQuery);
	GPOS_ASSERT(NULL != pdrgpdxlnQueryOutput);
	GPOS_ASSERT(NULL != poconf);

	BOOL fMinidump = GPOS_FTRACE(EopttraceMinidump);

	CMiniDumperDXL mdmp(pmp);
	if (fMinidump)
	{
		mdmp.Init();
	}

	CDXLNode *pdxlnPlan = NULL;
	CErrorHandlerStandard errhdl;
	GPOS_TRY_HDL(&errhdl)
	{
		CSerializableStackTrace serStack;
		CSerializableOptimizerConfig serOptConfig(poconf);
		CSerializableMDAccessor serMDA(pmda);
		CSerializableQuery serQuery(pdxlnQuery, pdrgpdxlnQueryOutput, pdrgpdxlnCTE);
		
		if (fMinidump)
		{
			// pre-serialize query and optimizer configurations
			serStack.AllocateBuffer(pmp);
			serOptConfig.Serialize(pmp);
			serQuery.Serialize(pmp);
		}

		{			
			poconf->AddRef();
			if (NULL != pceeval)
			{
				pceeval->AddRef();
			}

			// install opt context in TLS
			CAutoOptCtxt aoc(pmp, pmda, pceeval, poconf);

			// translate DXL Tree -> Expr Tree
			CTranslatorDXLToExpr dxltr(pmp, pmda);
			CExpression *pexprTranslated =	dxltr.PexprTranslateQuery(pdxlnQuery, pdrgpdxlnQueryOutput, pdrgpdxlnCTE);
			GPOS_CHECK_ABORT;
			gpdxl::DrgPul *pdrgpul = dxltr.PdrgpulOutputColRefs();
			gpmd::DrgPmdname *pdrgpmdname = dxltr.Pdrgpmdname();

			CQueryContext *pqc = CQueryContext::PqcGenerate(pmp, pexprTranslated, pdrgpul, pdrgpmdname, true /*fDeriveStats*/);
			GPOS_CHECK_ABORT;

			PrintQueryOrPlan(pmp, pexprTranslated, pqc);

			CWStringDynamic strTrace(pmp);
			COstreamString oss(&strTrace);

			// if the number of inlinable CTEs is greater than the cutoff, then
			// disable inlining for this query
			if (!GPOS_FTRACE(EopttraceEnableCTEInlining) ||
				CUtils::UlInlinableCTEs(pexprTranslated) > poconf->Pcteconf()->UlCTEInliningCutoff())
			{
				COptCtxt::PoctxtFromTLS()->Pcteinfo()->DisableInlining();
			}

			GPOS_CHECK_ABORT;
			// optimize logical expression tree into physical expression tree.
			CExpression *pexprPlan = PexprOptimize(pmp, pqc, pdrgpss);
			GPOS_CHECK_ABORT;

			PrintQueryOrPlan(pmp, pexprPlan);

			// translate plan into DXL
			pdxlnPlan = Pdxln(pmp, pmda, pexprPlan, pqc->PdrgPcr(), pdrgpmdname, ulHosts);
			GPOS_CHECK_ABORT;

			if (fMinidump)
			{
				CSerializablePlan serPlan(pdxlnPlan, poconf->Pec()->UllPlanId(), poconf->Pec()->UllPlanSpaceSize());
				serPlan.Serialize(pmp);
				CMinidumperUtils::Finalize(&mdmp, true /* fSerializeErrCtxt*/, ulSessionId, ulCmdId, szMinidumpFileName);
				GPOS_CHECK_ABORT;
			}
			
			if (GPOS_FTRACE(EopttraceSamplePlans))
			{
				DumpSamples(pmp, poconf->Pec(), ulSessionId, ulCmdId);
				GPOS_CHECK_ABORT;
			}

			// cleanup
			pexprTranslated->Release();
			pexprPlan->Release();
			GPOS_DELETE(pqc);
		}
	}
	GPOS_CATCH_EX(ex)
	{
		if (fMinidump)
		{
			CMinidumperUtils::Finalize(&mdmp, false /* fSerializeErrCtxt*/, ulSessionId, ulCmdId, szMinidumpFileName);
			HandleExceptionAfterFinalizingMinidump(ex);
		}

		GPOS_RETHROW(ex);
	}
	GPOS_CATCH_END;

	return pdxlnPlan;
}

//---------------------------------------------------------------------------
//	@function:
//		COptimizer::HandleExceptionAfterFinalizingMinidump
//
//	@doc:
//		Handle exception after finalizing minidump
//
//---------------------------------------------------------------------------
void
COptimizer::HandleExceptionAfterFinalizingMinidump
	(
	CException &ex
	)
{
	if (NULL != ITask::PtskSelf() &&
		!ITask::PtskSelf()->Perrctxt()->FPending())
	{
		// if error context has no pending exception, then minidump creation
		// might have reset the error,
		// in this case we need to raise the original exception
		GPOS_RAISE
			(
			ex.UlMajor(),
			ex.UlMinor(),
			GPOS_WSZ_LIT("re-raising exception after finalizing minidump")
			);
	}

	// otherwise error is still pending, re-throw original exception
	GPOS_RETHROW(ex);
}


//---------------------------------------------------------------------------
//	@function:
//		COptimizer::PexprOptimize
//
//	@doc:
//		Optimize query in given query context
//
//---------------------------------------------------------------------------
CExpression *
COptimizer::PexprOptimize
	(
	IMemoryPool *pmp,
	CQueryContext *pqc,
	DrgPss *pdrgpss
	)
{
	CEngine eng(pmp);
	eng.Init(pqc, pdrgpss);
	eng.Optimize();

	GPOS_CHECK_ABORT;

	CExpression *pexprPlan = eng.PexprExtractPlan();
	(void) pexprPlan->PrppCompute(pmp, pqc->Prpp());

	GPOS_CHECK_ABORT;

	return pexprPlan;
}


//---------------------------------------------------------------------------
//	@function:
//		COptimizer::Pdxln
//
//	@doc:
//		Translate an optimizer expression into a DXL tree 
//
//---------------------------------------------------------------------------
CDXLNode *
COptimizer::Pdxln
	(
	IMemoryPool *pmp, 
	CMDAccessor *pmda, 
	CExpression *pexpr,
	DrgPcr *pdrgpcr,
	DrgPmdname *pdrgpmdname,
	ULONG ulHosts
	)
{
	GPOS_ASSERT(0 < ulHosts);
	DrgPi *pdrgpiHosts = GPOS_NEW(pmp) DrgPi(pmp);

	for (ULONG ul = 0; ul < ulHosts; ul++)
	{
		pdrgpiHosts->Append(GPOS_NEW(pmp) INT(ul));
	}

	CTranslatorExprToDXL ptrexprtodxl(pmp, pmda, pdrgpiHosts);
	CDXLNode *pdxlnPlan = ptrexprtodxl.PdxlnTranslate(pexpr, pdrgpcr, pdrgpmdname);
	
	return pdxlnPlan;
}

// EOF
