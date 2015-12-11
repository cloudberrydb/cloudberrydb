//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CAutoOptCtxt.cpp
//
//	@doc:
//		Implementation of auto opt context
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/base/CAutoOptCtxt.h"
#include "gpopt/cost/ICostModel.h"
#include "gpopt/eval/CConstExprEvaluatorDefault.h"
#include "gpopt/optimizer/COptimizerConfig.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CAutoOptCtxt::CAutoOptCtxt
//
//	@doc:
//		ctor
//		Create and install default optimizer context
//
//---------------------------------------------------------------------------
CAutoOptCtxt::CAutoOptCtxt
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	IConstExprEvaluator *pceeval,
	COptimizerConfig *poconf
	)
{
	if (NULL == poconf)
	{
		// create default statistics configuration
		poconf = COptimizerConfig::PoconfDefault(pmp);
	}
	if (NULL == pceeval)
	{
		// use the default constant expression evaluator which cannot evaluate any expression
		pceeval = GPOS_NEW(pmp) CConstExprEvaluatorDefault();
	}

	COptCtxt *poctxt = COptCtxt::PoctxtCreate(pmp, pmda, pceeval, poconf);
	ITask::PtskSelf()->Tls().Store(poctxt);
}

//---------------------------------------------------------------------------
//	@function:
//		CAutoOptCtxt::CAutoOptCtxt
//
//	@doc:
//		ctor
//		Create and install default optimizer context with the given cost model
//
//---------------------------------------------------------------------------
CAutoOptCtxt::CAutoOptCtxt
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	IConstExprEvaluator *pceeval,
	ICostModel *pcm
	)
{
	GPOS_ASSERT(NULL != pcm);
	
	// create default statistics configuration
	COptimizerConfig *poconf = COptimizerConfig::PoconfDefault(pmp, pcm);
	
	if (NULL == pceeval)
	{
		// use the default constant expression evaluator which cannot evaluate any expression
		pceeval = GPOS_NEW(pmp) CConstExprEvaluatorDefault();
	}

	COptCtxt *poctxt = COptCtxt::PoctxtCreate(pmp, pmda, pceeval, poconf);
	ITask::PtskSelf()->Tls().Store(poctxt);
}

//---------------------------------------------------------------------------
//	@function:
//		CAutoOptCtxt::~CAutoOptCtxt
//
//	@doc:
//		dtor
//		uninstall optimizer context and destroy
//
//---------------------------------------------------------------------------
CAutoOptCtxt::~CAutoOptCtxt()
{
	CTaskLocalStorageObject *ptlsobj = ITask::PtskSelf()->Tls().Ptlsobj(CTaskLocalStorage::EtlsidxOptCtxt);
	ITask::PtskSelf()->Tls().Remove(ptlsobj);
	
	GPOS_DELETE(ptlsobj);
}

// EOF

