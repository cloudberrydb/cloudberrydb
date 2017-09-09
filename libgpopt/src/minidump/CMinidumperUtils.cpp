//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMinidumperUtils.cpp
//
//	@doc:
//		Implementation of minidump utility functions
//---------------------------------------------------------------------------

#include <fcntl.h>

#include "gpos/base.h"
#include "gpos/common/CAutoTimer.h"
#include "gpos/common/CAutoRef.h"
#include "gpos/common/CBitSet.h"
#include "gpos/common/syslibwrapper.h"
#include "gpos/error/CAutoTrace.h"
#include "gpos/error/CErrorContext.h"
#include "gpos/error/CErrorHandlerStandard.h"
#include "gpos/io/CFileWriter.h"
#include "gpos/io/ioutils.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/task/CAutoSuspendAbort.h"
#include "gpos/task/CAutoTraceFlag.h"
#include "gpos/task/CTask.h"
#include "gpos/task/CWorker.h"

#include "naucrates/traceflags/traceflags.h"
#include "naucrates/dxl/parser/CParseHandlerDXL.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/md/CMDProviderMemory.h"

#include "gpopt/base/CAutoOptCtxt.h"
#include "gpopt/cost/ICostModel.h"
#include "gpopt/engine/CEngine.h"
#include "gpopt/engine/CEnumeratorConfig.h"
#include "gpopt/engine/CStatisticsConfig.h"
#include "gpopt/mdcache/CMDAccessor.h"
#include "gpopt/mdcache/CMDCache.h"
#include "gpopt/minidump/CMetadataAccessorFactory.h"
#include "gpopt/minidump/CMinidumperUtils.h"
#include "gpopt/minidump/CMiniDumperDXL.h"
#include "gpopt/optimizer/COptimizer.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "gpopt/translate/CTranslatorDXLToExpr.h"

#include <fstream>

using namespace gpos;
using namespace gpopt;
using namespace gpdxl;
using namespace std;

//---------------------------------------------------------------------------
//	@function:
//		CMinidumperUtils::PdxlmdLoad
//
//	@doc:
//		Load minidump file
//
//---------------------------------------------------------------------------
CDXLMinidump *
CMinidumperUtils::PdxlmdLoad
	(
	IMemoryPool *pmp,
	const CHAR *szFileName
	)
{
	CAutoTraceFlag atf1(EtraceSimulateAbort, false);
	CAutoTraceFlag atf2(EtraceSimulateOOM, false);

	{
		CAutoTrace at(pmp);
		at.Os() << "parsing DXL File " << szFileName;
	}
	
	CParseHandlerDXL *pphdxl = CDXLUtils::PphdxlParseDXLFile(pmp, szFileName, NULL /*szXSDPath*/);

	CBitSet *pbs = pphdxl->Pbs();
	COptimizerConfig *poconf = pphdxl->Poconf();
	CDXLNode *pdxlnQuery = pphdxl->PdxlnQuery();
	DrgPdxln *pdrgpdxlnQueryOutput = pphdxl->PdrgpdxlnOutputCols();
	DrgPdxln *pdrgpdxlnCTE = pphdxl->PdrgpdxlnCTE();
	DrgPimdobj *pdrgpmdobj = pphdxl->Pdrgpmdobj();
	DrgPsysid *pdrgpsysid = pphdxl->Pdrgpsysid();
	CDXLNode *pdxlnPlan = pphdxl->PdxlnPlan();
	ULLONG ullPlanId = pphdxl->UllPlanId();
	ULLONG ullPlanSpaceSize = pphdxl->UllPlanSpaceSize();

	if (NULL != pbs)
	{
		pbs->AddRef();
	}
	
	if (NULL != poconf)
	{
		poconf->AddRef();
	}

	if (NULL != pdxlnQuery)
	{
		pdxlnQuery->AddRef();
	}
	
	if (NULL != pdrgpdxlnQueryOutput)
	{
		pdrgpdxlnQueryOutput->AddRef();
	}

	if (NULL != pdrgpdxlnCTE)
	{
		pdrgpdxlnCTE->AddRef();
	}

	if (NULL != pdrgpmdobj)
	{
		pdrgpmdobj->AddRef();
	}
	
	if (NULL != pdrgpsysid)
	{
		pdrgpsysid->AddRef();
	}

	if (NULL != pdxlnPlan)
	{
		pdxlnPlan->AddRef();
	}
	
	// cleanup
	GPOS_DELETE(pphdxl);
	
	// reset time slice
#ifdef GPOS_DEBUG
    CWorker::PwrkrSelf()->ResetTimeSlice();
#endif // GPOS_DEBUG

	return GPOS_NEW(pmp) CDXLMinidump
				(
				pbs,
				poconf,
				pdxlnQuery,
				pdrgpdxlnQueryOutput,
				pdrgpdxlnCTE,
				pdxlnPlan,
				pdrgpmdobj,
				pdrgpsysid,
				ullPlanId,
				ullPlanSpaceSize
				);
}


//---------------------------------------------------------------------------
//	@function:
//		CMinidumperUtils::GenerateMinidumpFileName
//
//	@doc:
//		Generate a timestamp-based minidump filename in the provided buffer.
//
//---------------------------------------------------------------------------
void
CMinidumperUtils::GenerateMinidumpFileName
	(
	CHAR *szBuf,
	ULONG ulLength,
	ULONG ulSessionId,
	ULONG ulCmdId,
	const CHAR *szMinidumpFileName // name of minidump file to be created,
									// if NULL, a time-based name is generated
	)
{
	if (!gpos::ioutils::FPathExist("minidumps"))
	{
		GPOS_TRY
		{
			// create a minidumps folder
			const ULONG ulWrPerms = S_IRUSR  | S_IWUSR  | S_IXUSR;
			gpos::ioutils::MkDir("minidumps", ulWrPerms);
		}
		GPOS_CATCH_EX(ex)
		{
			std::cerr << "[OPT]: Failed to create minidumps directory";

			// ignore exceptions during the creation of directory
			GPOS_RESET_EX;
		}
		GPOS_CATCH_END;
	}

	if (NULL == szMinidumpFileName)
	{
		// generate a time-based file name
		CUtils::GenerateFileName(szBuf, "minidumps/Minidump", "mdp", ulLength, ulSessionId, ulCmdId);
	}
	else
	{
		// use provided file name
		const CHAR *szPrefix = "minidumps/";
		const ULONG ulPrefixLength = clib::UlStrLen(szPrefix);
		clib::SzStrNCpy(szBuf, szPrefix, ulPrefixLength);

		// remove directory path before file name, if any
		ULONG ulNameLength = clib::UlStrLen(szMinidumpFileName);
		ULONG ulNameStart  = ulNameLength - 1;
		while (ulNameStart > 0 &&
				szMinidumpFileName[ulNameStart - 1] != '\\' &&
				szMinidumpFileName[ulNameStart - 1] != '/')
		{
			ulNameStart --;
		}

		ulNameLength = clib::UlStrLen(szMinidumpFileName + ulNameStart);
		clib::SzStrNCpy(szBuf + ulPrefixLength, szMinidumpFileName + ulNameStart, ulNameLength);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CMinidumperUtils::Finalize
//
//	@doc:
//		Finalize minidump and dump to file
//
//---------------------------------------------------------------------------
void
CMinidumperUtils::Finalize
	(
	CMiniDumperDXL *pmdmp,
	BOOL fSerializeErrCtx
	)
{
	CAutoTraceFlag atf1(EtraceSimulateAbort, false);
	CAutoTraceFlag atf2(EtraceSimulateOOM, false);
	CAutoTraceFlag atf3(EtraceSimulateNetError, false);
	CAutoTraceFlag atf4(EtraceSimulateIOError, false);

	if (fSerializeErrCtx)
	{
		CErrorContext *perrctxt = CTask::PtskSelf()->PerrctxtConvert();
		perrctxt->Serialize();
	}
	
	pmdmp->Finalize();
}

//---------------------------------------------------------------------------
//	@function:
//		CMinidumperUtils::PdxlnExecuteMinidump
//
//	@doc:
//		Load and execute the minidump in the given file
//
//---------------------------------------------------------------------------
CDXLNode * 
CMinidumperUtils::PdxlnExecuteMinidump
	(
	IMemoryPool *pmp,
	const CHAR *szFileName,
	ULONG ulSegments,
	ULONG ulSessionId,
	ULONG ulCmdId,
	COptimizerConfig *poconf,
	IConstExprEvaluator *pceeval
	)
{
	GPOS_ASSERT(NULL != szFileName);
	GPOS_ASSERT(NULL != poconf);

	CAutoTimer at("Minidump", true /*fPrint*/);

	// load dump file
	CDXLMinidump *pdxlmd = CMinidumperUtils::PdxlmdLoad(pmp, szFileName);
	GPOS_CHECK_ABORT;

	CDXLNode *pdxlnPlan = PdxlnExecuteMinidump
							(
							pmp,
							pdxlmd,
							szFileName,
							ulSegments,
							ulSessionId,
							ulCmdId,
							poconf,
							pceeval
							);

	// cleanup
	GPOS_DELETE(pdxlmd);
	
	// reset time slice
#ifdef GPOS_DEBUG
    CWorker::PwrkrSelf()->ResetTimeSlice();
#endif // GPOS_DEBUG
    
	return pdxlnPlan;
}


//---------------------------------------------------------------------------
//	@function:
//		CMinidumperUtils::PdxlnExecuteMinidump
//
//	@doc:
//		Execute the given minidump
//
//---------------------------------------------------------------------------
CDXLNode * 
CMinidumperUtils::PdxlnExecuteMinidump
	(
	IMemoryPool *pmp,
	CDXLMinidump *pdxlmd,
	const CHAR *szFileName,
	ULONG ulSegments, 
	ULONG ulSessionId,
	ULONG ulCmdId,
	COptimizerConfig *poconf,
	IConstExprEvaluator *pceeval
	)
{
	GPOS_ASSERT(NULL != szFileName);

	// reset metadata ccache
	CMDCache::Reset();

	CMetadataAccessorFactory factory(pmp, pdxlmd, szFileName);

	CDXLNode *result = CMinidumperUtils::PdxlnExecuteMinidump(pmp, factory.Pmda(), pdxlmd, szFileName, ulSegments, ulSessionId, ulCmdId, poconf, pceeval);

	return result;
}


//---------------------------------------------------------------------------
//	@function:
//		CMinidumperUtils::PdxlnExecuteMinidump
//
//	@doc:
//		Execute the given minidump using the given MD accessor
//
//---------------------------------------------------------------------------
CDXLNode *
CMinidumperUtils::PdxlnExecuteMinidump
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	CDXLMinidump *pdxlmd,
	const CHAR *szFileName,
	ULONG ulSegments,
	ULONG ulSessionId,
	ULONG ulCmdId,
	COptimizerConfig *poconf,
	IConstExprEvaluator *pceeval
	)
{
	GPOS_ASSERT(NULL != pmda);
	GPOS_ASSERT(NULL != pdxlmd->PdxlnQuery() &&
				NULL != pdxlmd->PdrgpdxlnQueryOutput() &&
				NULL != pdxlmd->PdrgpdxlnCTE() &&
				"No query found in Minidump");
	GPOS_ASSERT(NULL != pdxlmd->Pdrgpmdobj() && NULL != pdxlmd->Pdrgpsysid() && "No metadata found in Minidump");
	GPOS_ASSERT(NULL != poconf);

	CDXLNode *pdxlnPlan = NULL;
	CAutoTimer at("Minidump", true /*fPrint*/);

	GPOS_CHECK_ABORT;

	// set trace flags
	CBitSet *pbsEnabled = NULL;
	CBitSet *pbsDisabled = NULL;
	SetTraceflags(pmp, pdxlmd->Pbs(), &pbsEnabled, &pbsDisabled);

	if (NULL == pceeval)
	{
		// disable constant expression evaluation when running minidump since
		// there no executor to compute the scalar expression
		GPOS_UNSET_TRACE(EopttraceEnableConstantExpressionEvaluation);
	}

	CErrorHandlerStandard errhdl;
	GPOS_TRY_HDL(&errhdl)
	{
		pdxlnPlan = COptimizer::PdxlnOptimize
								(
								pmp,
								pmda,
								pdxlmd->PdxlnQuery(),
								pdxlmd->PdrgpdxlnQueryOutput(),
								pdxlmd->PdrgpdxlnCTE(),
								pceeval,
								ulSegments,
								ulSessionId,
								ulCmdId,
								NULL, // pdrgpss
								poconf,
								szFileName
								);
	}
	GPOS_CATCH_EX(ex)
	{
		// reset trace flags
		ResetTraceflags(pbsEnabled, pbsDisabled);

		CRefCount::SafeRelease(pbsEnabled);
		CRefCount::SafeRelease(pbsDisabled);

		GPOS_RETHROW(ex);
	}
	GPOS_CATCH_END;

	// reset trace flags
	ResetTraceflags(pbsEnabled, pbsDisabled);
	
	// clean up
	CRefCount::SafeRelease(pbsEnabled);
	CRefCount::SafeRelease(pbsDisabled);

	GPOS_CHECK_ABORT;

	return pdxlnPlan;
}

// EOF
