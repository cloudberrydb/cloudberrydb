//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerManagerTest.cpp
//
//	@doc:
//		Tests parsing DXL documents into DXL trees.
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/io/COstreamString.h"
#include "gpos/string/CWStringDynamic.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/task/CAutoTraceFlag.h"
#include "gpos/test/CUnittest.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/parser/CParseHandlerHashJoin.h"
#include "naucrates/dxl/operators/CDXLPhysicalTableScan.h"
#include "naucrates/dxl/parser/CParseHandlerPlan.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"
#include "naucrates/dxl/xml/CDXLMemoryManager.h"

#include <xercesc/sax2/XMLReaderFactory.hpp>

#include "unittest/dxl/CParseHandlerManagerTest.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerManagerTest::EresUnittest
//
//	@doc:
//		Unittest for activating and deactivating DXL parse handlers
//
//---------------------------------------------------------------------------
GPOS_RESULT
CParseHandlerManagerTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CParseHandlerManagerTest::EresUnittest_Basic)
		};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerManagerTest::EresUnittest_Basic
//
//	@doc:
//		Testing activation and deactivation of parse handlers
//
//---------------------------------------------------------------------------
GPOS_RESULT
CParseHandlerManagerTest::EresUnittest_Basic()
{
	// create memory pool
	CAutoMemoryPool amp(CAutoMemoryPool::ElcNone);
	IMemoryPool *pmp = amp.Pmp();
		
	// create XML reader and a parse handler manager for it
	CDXLMemoryManager *pmm = GPOS_NEW(pmp) CDXLMemoryManager(pmp);

	SAX2XMLReader* parser = NULL;
	{
		CAutoTraceFlag atf(EtraceSimulateOOM, false);
		parser = XMLReaderFactory::createXMLReader(pmm);
	}

	CParseHandlerManager *pphm = GPOS_NEW(pmp) CParseHandlerManager(pmm, parser);
	
	// create some parse handlers
	CParseHandlerPlan *pphPlan = GPOS_NEW(pmp) CParseHandlerPlan(pmp, pphm, NULL);
	CParseHandlerHashJoin *pphHJ = GPOS_NEW(pmp) CParseHandlerHashJoin(pmp, pphm, pphPlan);
	
	pphm->ActivateParseHandler(pphPlan);
	GPOS_ASSERT(pphPlan == pphm->PphCurrent());
	GPOS_ASSERT(pphPlan == parser->getContentHandler());

	pphm->ActivateParseHandler(pphHJ);
	GPOS_ASSERT(pphHJ == pphm->PphCurrent());
	GPOS_ASSERT(pphHJ == parser->getContentHandler());


	pphm->DeactivateHandler();
	GPOS_ASSERT(pphPlan == pphm->PphCurrent());
	GPOS_ASSERT(pphPlan == parser->getContentHandler());
	
	pphm->DeactivateHandler();
	// no more parse handlers
	GPOS_ASSERT(NULL == pphm->PphCurrent());
	GPOS_ASSERT(NULL == parser->getContentHandler());

	// cleanup
	GPOS_DELETE(pphm);
	delete parser;
	GPOS_DELETE(pmm);
	GPOS_DELETE(pphPlan);
	GPOS_DELETE(pphHJ);

	return GPOS_OK;
}





// EOF
