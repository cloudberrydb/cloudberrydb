//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLUtils.cpp
//
//	@doc:
//		Implementation of the utility methods for parsing and searializing DXL.
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/common/CAutoTimer.h"
#include "gpos/common/CAutoRef.h"
#include "gpos/common/CAutoRg.h"

#include "gpos/io/ioutils.h"
#include "gpos/io/CFileReader.h"
#include "gpos/io/COstreamString.h"
#include "gpos/task/CAutoTraceFlag.h"
#include "gpos/task/CWorker.h"
#include "gpos/task/CTraceFlagIter.h"

#include "gpopt/engine/CEnumeratorConfig.h"
#include "gpopt/engine/CStatisticsConfig.h"
#include "gpopt/optimizer/COptimizerConfig.h"

#include "naucrates/base/CQueryToDXLResult.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/parser/CParseHandlerDXL.h"
#include "naucrates/dxl/parser/CParseHandlerPlan.h"

#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"
#include "naucrates/dxl/parser/CParseHandlerDummy.h"
#include "naucrates/dxl/xml/CDXLMemoryManager.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"
#include "gpopt/mdcache/CMDAccessor.h"
#include "gpopt/base/COptCtxt.h"

#include "naucrates/md/CMDRequest.h"
#include "naucrates/md/CDXLStatsDerivedRelation.h"

#include "naucrates/traceflags/traceflags.h"

#include <xercesc/sax2/SAX2XMLReader.hpp>
#include <xercesc/sax2/XMLReaderFactory.hpp>
#include <xercesc/framework/MemBufInputSource.hpp>
#include <xercesc/util/XMLString.hpp>
#include <xercesc/util/Base64.hpp>

using namespace gpdxl;
using namespace gpmd;
using namespace gpos;
using namespace gpopt;

XERCES_CPP_NAMESPACE_USE



//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::PphdxlParseDXL
//
//	@doc:
//		Start the parsing of the given DXL string and return the top-level parser.
//		If a non-empty XSD schema location is provided, the DXL is validated against
//		that schema, and an exception is thrown if the DXL does not conform.
//
//---------------------------------------------------------------------------
CParseHandlerDXL *
CDXLUtils::PphdxlParseDXL
	(
	IMemoryPool *pmp,
	const CHAR *szDXL,
	const CHAR *szXSDPath
	)
{
	GPOS_ASSERT(NULL != pmp);
	// we need to disable OOM simulation here, otherwise xerces throws ABORT signal
	CAutoTraceFlag atf(EtraceSimulateOOM, false);
	CAutoTraceFlag atf2(EtraceSimulateAbort, false);

	// setup own memory manager
	CDXLMemoryManager *pmm = GPOS_NEW(pmp) CDXLMemoryManager(pmp);
	SAX2XMLReader* pxmlreader = XMLReaderFactory::createXMLReader(pmm);

#ifdef GPOS_DEBUG
	CWorker::PwrkrSelf()->ResetTimeSlice();
#endif // GPOS_DEBUG

	XMLCh *xmlszXSDPath = NULL;

	if (NULL != szXSDPath)
	{
		// setup XSD validation
		pxmlreader->setFeature(XMLUni::fgSAX2CoreValidation, true);
		pxmlreader->setFeature(XMLUni::fgXercesDynamic, false);
		pxmlreader->setFeature(XMLUni::fgSAX2CoreNameSpaces, true);
		pxmlreader->setFeature(XMLUni::fgXercesSchema, true);
		
		pxmlreader->setFeature(XMLUni::fgXercesSchemaFullChecking, true);
		pxmlreader->setFeature(XMLUni::fgSAX2CoreNameSpacePrefixes, true);
		
		xmlszXSDPath = XMLString::transcode(szXSDPath, pmm);
		pxmlreader->setProperty(XMLUni::fgXercesSchemaExternalSchemaLocation, (void*) xmlszXSDPath);
	}
			
	CParseHandlerManager *pphm = GPOS_NEW(pmp) CParseHandlerManager(pmm, pxmlreader);

	CParseHandlerDXL *pphdxl = CParseHandlerFactory::Pphdxl(pmp, pphm);

	pphm->ActivateParseHandler(pphdxl);
		
	MemBufInputSource *pmbis = new(pmm) MemBufInputSource(
				(const XMLByte*) szDXL,
				strlen(szDXL),
				"dxl test",
				false,
				pmm
	    	);

#ifdef GPOS_DEBUG
	CWorker::PwrkrSelf()->ResetTimeSlice();
#endif // GPOS_DEBUG

	try
	{
		pxmlreader->parse(*pmbis);
	}
	catch (const XMLException&)
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLXercesParseError);
		return NULL;
	}
	catch (const SAXParseException&)
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLXercesParseError);
		return NULL;
	}
	catch (const SAXException&)
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLXercesParseError);
		return NULL;
	}


#ifdef GPOS_DEBUG
	CWorker::PwrkrSelf()->ResetTimeSlice();
#endif // GPOS_DEBUG

	GPOS_CHECK_ABORT;

	// cleanup
	delete pxmlreader;
	delete pmbis;
	GPOS_DELETE(pphm);
	GPOS_DELETE(pmm);
	delete xmlszXSDPath;

	// reset time slice counter as unloading deleting Xerces SAX2 readers seems to take a lot of time (OPT-491)
#ifdef GPOS_DEBUG
	CWorker::PwrkrSelf()->ResetTimeSlice();
#endif // GPOS_DEBUG

	GPOS_CHECK_ABORT;

	return pphdxl;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::PphdxlParseDXLFile
//
//	@doc:
//		Start the parsing of the given DXL string and return the top-level parser.
//		If a non-empty XSD schema location is provided, the DXL is validated against
//		that schema, and an exception is thrown if the DXL does not conform.
//
//---------------------------------------------------------------------------
CParseHandlerDXL *
CDXLUtils::PphdxlParseDXLFile
	(
	IMemoryPool *pmp,
	const CHAR *szDXLFileName,
	const CHAR *szXSDPath
	)
{
	GPOS_ASSERT(NULL != pmp);
		
	// setup own memory manager
	CDXLMemoryManager mm(pmp);
	SAX2XMLReader* pxmlreader = NULL;
	{
		// we need to disable OOM simulation here, otherwise xerces throws ABORT signal
		CAutoTraceFlag atf(EtraceSimulateOOM, false);
		CAutoTraceFlag atf2(EtraceSimulateAbort, false);

		pxmlreader = XMLReaderFactory::createXMLReader(&mm);
	}
	
	XMLCh *xmlszXSDPath = NULL;
	
	if (NULL != szXSDPath)
	{
		// setup XSD validation
		pxmlreader->setFeature(XMLUni::fgSAX2CoreValidation, true);
		pxmlreader->setFeature(XMLUni::fgXercesDynamic, false);
		pxmlreader->setFeature(XMLUni::fgSAX2CoreNameSpaces, true);
		pxmlreader->setFeature(XMLUni::fgXercesSchema, true);
		
		pxmlreader->setFeature(XMLUni::fgXercesSchemaFullChecking, true);
		pxmlreader->setFeature(XMLUni::fgSAX2CoreNameSpacePrefixes, true);
		
		xmlszXSDPath = XMLString::transcode(szXSDPath, &mm);
		pxmlreader->setProperty(XMLUni::fgXercesSchemaExternalSchemaLocation, (void*) xmlszXSDPath);
	}

	CParseHandlerManager phm(&mm, pxmlreader);
	CParseHandlerDXL *pph = CParseHandlerFactory::Pphdxl(pmp, &phm);
	phm.ActivateParseHandler(pph);
	GPOS_CHECK_ABORT;

	try
	{
		CAutoTraceFlag atf(EtraceSimulateOOM, false);
		CAutoTraceFlag atf2(EtraceSimulateAbort, false);
		GPOS_CHECK_ABORT;

		pxmlreader->parse(szDXLFileName);

		// reset time slice
#ifdef GPOS_DEBUG
	    CWorker::PwrkrSelf()->ResetTimeSlice();
#endif // GPOS_DEBUG
	}
	catch (const XMLException&)
	{
		GPOS_DELETE(pph);
		delete pxmlreader;
		delete[] xmlszXSDPath;
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLXercesParseError);

		return NULL;
	}
	catch (const SAXParseException&ex)
	{
		GPOS_DELETE(pph);
		delete pxmlreader;
		delete[] xmlszXSDPath;
		
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLXercesParseError);

		return NULL;
	}
	catch (const SAXException&)
	{
		GPOS_DELETE(pph);
		delete pxmlreader;
		delete[] xmlszXSDPath;
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLXercesParseError);

		return NULL;
	}

	GPOS_CHECK_ABORT;

	// cleanup
	delete pxmlreader;
	
	// reset time slice counter as unloading deleting Xerces SAX2 readers seems to take a lot of time (OPT-491)
#ifdef GPOS_DEBUG
    CWorker::PwrkrSelf()->ResetTimeSlice();
#endif // GPOS_DEBUG
    
	delete[] xmlszXSDPath;
	
	return pph;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::PphdxlParseDXL
//
//	@doc:
//		Start the parsing of the given DXL string and return the top-level parser.
//		If a non-empty XSD schema location is provided, the DXL is validated against
//		that schema, and an exception is thrown if the DXL does not conform.
//
//---------------------------------------------------------------------------
CParseHandlerDXL *
CDXLUtils::PphdxlParseDXL
	(
	IMemoryPool *pmp,
	const CWStringBase *pstr,
	const CHAR *szXSDPath
	)
{
	CAutoRg<CHAR> a_sz;
	a_sz = SzFromWsz(pmp, pstr->Wsz());
	CParseHandlerDXL *pphdxl = PphdxlParseDXL(pmp, a_sz.Rgt(), szXSDPath);
	return pphdxl;
}



//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::PdxlnParsePlan
//
//	@doc:
//		Parse DXL string into a DXL plan tree.
//		If a non-empty XSD schema location is provided, the DXL is validated against
//		that schema, and an exception is thrown if the DXL does not conform.
//
//---------------------------------------------------------------------------
CDXLNode *
CDXLUtils::PdxlnParsePlan
	(
	IMemoryPool *pmp,
	const CHAR *szDXL,
	const CHAR *szXSDPath,
	ULLONG *pullPlanId,
	ULLONG *pullPlanSpaceSize
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pullPlanId);
	GPOS_ASSERT(NULL != pullPlanSpaceSize);

	// create and install a parse handler for the DXL document
	CParseHandlerDXL *pphdxl = PphdxlParseDXL(pmp, szDXL, szXSDPath);
	CAutoP<CParseHandlerDXL> a_pphdxl(pphdxl);
	
	GPOS_ASSERT(NULL != a_pphdxl.Pt());
	
	// collect plan info from dxl parse handler
	CDXLNode *pdxlnRoot = a_pphdxl->PdxlnPlan();
	*pullPlanId = a_pphdxl->UllPlanId();
	*pullPlanSpaceSize = a_pphdxl->UllPlanSpaceSize();
	
	GPOS_ASSERT(NULL != pdxlnRoot);
	
#ifdef GPOS_DEBUG
	pdxlnRoot->Pdxlop()->AssertValid(pdxlnRoot, true /* fValidateChildren */);
#endif
	
	pdxlnRoot->AddRef();
	
	return pdxlnRoot;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::PdxlnParseDXLQuery
//
//	@doc:
//		Parse DXL string representing the query into
//		1. a DXL tree representing the query
//		2. a DXL tree representing the query output
//		If a non-empty XSD schema location is provided, the DXL is validated against
//		that schema, and an exception is thrown if the DXL does not conform.
//
//---------------------------------------------------------------------------
CQueryToDXLResult *
CDXLUtils::PdxlnParseDXLQuery
	(
	IMemoryPool *pmp,
	const CHAR *szDXL,
	const CHAR *szXSDPath
	)
{
	GPOS_ASSERT(NULL != pmp);

	// create and install a parse handler for the DXL document
	CParseHandlerDXL *pphdxl = PphdxlParseDXL(pmp, szDXL, szXSDPath);
	CAutoP<CParseHandlerDXL> a_pphdxl(pphdxl);

	// collect dxl tree of the query from dxl parse handler
	CDXLNode *pdxlnRoot = pphdxl->PdxlnQuery();
	GPOS_ASSERT(NULL != pdxlnRoot);
	
#ifdef GPOS_DEBUG
	pdxlnRoot->Pdxlop()->AssertValid(pdxlnRoot, true /* fValidateChildren */);
#endif
		
	pdxlnRoot->AddRef();

	// collect the list of query output columns from the dxl parse handler
	GPOS_ASSERT(NULL != pphdxl->PdrgpdxlnOutputCols());
	DrgPdxln *pdrgpdxlnQO = pphdxl->PdrgpdxlnOutputCols();
	pdrgpdxlnQO->AddRef();

	// collect the list of CTEs
	DrgPdxln *pdrgpdxlnCTE = pphdxl->PdrgpdxlnCTE();
	GPOS_ASSERT(NULL != pdrgpdxlnCTE);
	pdrgpdxlnCTE->AddRef();

	CQueryToDXLResult *ptrOutput = GPOS_NEW(pmp) CQueryToDXLResult(pdxlnRoot, pdrgpdxlnQO, pdrgpdxlnCTE);

	return ptrOutput;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::PdxlnParseScalarExpr
//
//	@doc:
//		Parse a scalar expression as a top level node in a "ScalarExpr" tag.
//---------------------------------------------------------------------------
CDXLNode *
CDXLUtils::PdxlnParseScalarExpr
	(
	IMemoryPool *pmp,
	const CHAR *szDXL,
	const CHAR *szXSDPath
	)
{
	GPOS_ASSERT(NULL != pmp);

	// create and install a parse handler for the DXL document
	CAutoP<CParseHandlerDXL> a_pphdxl(PphdxlParseDXL(pmp, szDXL, szXSDPath));

	// collect dxl tree of the query from dxl parse handler
	CDXLNode *pdxlnRoot = a_pphdxl->PdxlnScalarExpr();
	GPOS_ASSERT(NULL != pdxlnRoot);
	pdxlnRoot->AddRef();

	return pdxlnRoot;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::PdrgpmdobjParseDXL
//
//	@doc:
//		Parse a list of metadata objects from the given DXL string.
//		If a non-empty XSD schema location is provided, the DXL is validated against
//		that schema, and an exception is thrown if the DXL does not conform.
//
//---------------------------------------------------------------------------
DrgPimdobj *
CDXLUtils::PdrgpmdobjParseDXL
	(
	IMemoryPool *pmp,
	const CHAR *szDXL,
	const CHAR *szXSDPath
	)
{
	GPOS_ASSERT(NULL != pmp);

	// create and install a parse handler for the DXL document
	CParseHandlerDXL *pphdxl = PphdxlParseDXL(pmp, szDXL, szXSDPath);
	CAutoP<CParseHandlerDXL> a_pphdxl(pphdxl);
	
	// collect metadata objects from dxl parse handler
	DrgPimdobj *pdrgpmdobj = pphdxl->Pdrgpmdobj();
	pdrgpmdobj->AddRef();
	
	return pdrgpmdobj;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::PmdidParseDXL
//
//	@doc:
//		Parse an mdid from a DXL metadata document
//
//---------------------------------------------------------------------------
IMDId *
CDXLUtils::PmdidParseDXL
	(
	IMemoryPool *pmp,
	const CWStringBase *pstrDXL,
	const CHAR *szXSDPath
	)
{
	GPOS_ASSERT(NULL != pmp);

	// create and install a parse handler for the DXL document
	CParseHandlerDXL *pphdxl = PphdxlParseDXL(pmp, pstrDXL, szXSDPath);
	CAutoP<CParseHandlerDXL> a_pphdxl(pphdxl);
	
	// collect metadata objects from dxl parse handler
	DrgPmdid *pdrgpmdid = pphdxl->Pdrgpmdid();
	
	GPOS_ASSERT(1 == pdrgpmdid->UlLength());
	
	IMDId *pmdid = (*pdrgpmdid)[0];
	pmdid->AddRef();

	return pmdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::PmdrequestParseDXL
//
//	@doc:
//		Parse a metadata request.
//		If a non-empty XSD schema location is provided, the DXL is validated against
//		that schema, and an exception is thrown if the DXL does not conform.
//
//---------------------------------------------------------------------------
CMDRequest *
CDXLUtils::PmdrequestParseDXL
	(
	IMemoryPool *pmp,
	const CHAR *szDXL,
	const CHAR *szXSDPath
	)
{
	GPOS_ASSERT(NULL != pmp);

	// create and install a parse handler for the DXL document
	CParseHandlerDXL *pphdxl = PphdxlParseDXL(pmp, szDXL, szXSDPath);
	CAutoP<CParseHandlerDXL> a_pphdxl(pphdxl);

	// collect metadata ids from dxl parse handler
	CMDRequest *pmdr = pphdxl->Pmdr();
	GPOS_ASSERT(NULL != pmdr);
	pmdr->AddRef();

	return pmdr;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::PmdrequestParseDXL
//
//	@doc:
//		Parse an MD request from the given DXL string.
//		Same as above but with a wide-character input
//
//---------------------------------------------------------------------------
CMDRequest *
CDXLUtils::PmdrequestParseDXL
	(
	IMemoryPool *pmp,
	const WCHAR *wszDXL,
	const CHAR *szXSDPath
	)
{
	GPOS_ASSERT(NULL != pmp);

	CAutoRg<CHAR> a_szDXL(CDXLUtils::SzFromWsz(pmp, wszDXL));
	
	// create and install a parse handler for the DXL document
	CMDRequest *pmdr = PmdrequestParseDXL(pmp, a_szDXL.Rgt(), szXSDPath);

	return pmdr;
}

// parse optimizer config DXL
COptimizerConfig *
CDXLUtils::PoptimizerConfigParseDXL
	(
	IMemoryPool *pmp,
	const CHAR *szDXL,
	const CHAR *szXSDPath
	)
{
	GPOS_ASSERT(NULL != pmp);

	// create and install a parse handler for the DXL document
	CParseHandlerDXL *pphdxl = PphdxlParseDXL(pmp, szDXL, szXSDPath);
	// though we could access the traceflags member of the CParseHandlerDXL
	// here we don't access them or store them anywhere,
	// so, if using this function, note that any traceflags present
	// in the DXL being parsed will be discarded
	CAutoP<CParseHandlerDXL> a_pphdxl(pphdxl);

	// collect optimizer conf from dxl parse handler
	COptimizerConfig *poconf = pphdxl->Poconf();
	GPOS_ASSERT(NULL != poconf);
	poconf->AddRef();

	return poconf;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::PdrgpdxlstatsderrelParseDXL
//
//	@doc:
//		Parse a list of statistics objects from the given DXL string.
//		If a non-empty XSD schema location is provided, the DXL is validated against
//		that schema, and an exception is thrown if the DXL does not conform.
//
//---------------------------------------------------------------------------
DrgPdxlstatsderrel *
CDXLUtils::PdrgpdxlstatsderrelParseDXL
	(
	IMemoryPool *pmp,
	const CWStringBase *pstr,
	const CHAR *szXSDPath
	)
{
	GPOS_ASSERT(NULL != pmp);

	// create and install a parse handler for the DXL document
	CParseHandlerDXL *pphdxl = PphdxlParseDXL(pmp, pstr, szXSDPath);
	CAutoP<CParseHandlerDXL> a_pphdxl(pphdxl);

	// collect statistics objects from dxl parse handler
	DrgPdxlstatsderrel *pdrgpdxlstatsderrel = pphdxl->Pdrgpdxlstatsderrel();
	pdrgpdxlstatsderrel->AddRef();
	
	return pdrgpdxlstatsderrel;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::PdrgpdxlstatsderrelParseDXL
//
//	@doc:
//		Parse a list of statistics objects from the given DXL string.
//		If a non-empty XSD schema location is provided, the DXL is validated against
//		that schema, and an exception is thrown if the DXL does not conform.
//
//---------------------------------------------------------------------------
DrgPdxlstatsderrel *
CDXLUtils::PdrgpdxlstatsderrelParseDXL
	(
	IMemoryPool *pmp,
	const CHAR *szDXL,
	const CHAR *szXSDPath
	)
{
	GPOS_ASSERT(NULL != pmp);

	// create and install a parse handler for the DXL document
	CParseHandlerDXL *pphdxl = PphdxlParseDXL(pmp, szDXL, szXSDPath);
	CAutoP<CParseHandlerDXL> a_pphdxl(pphdxl);

	// collect statistics objects from dxl parse handler
	DrgPdxlstatsderrel *pdrgpdxlstatsderrel = pphdxl->Pdrgpdxlstatsderrel();
	pdrgpdxlstatsderrel->AddRef();
	
	return pdrgpdxlstatsderrel;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::PdrgpstatsTranslateStats
//
//	@doc:
//		Translate the array of dxl statistics objects to an array of 
//		optimizer statistics object.
//---------------------------------------------------------------------------
DrgPstats *
CDXLUtils::PdrgpstatsTranslateStats
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	DrgPdxlstatsderrel *pdrgpdxlstatsderrel
	)
{
	GPOS_ASSERT(NULL != pdrgpdxlstatsderrel);

	DrgPstats *pdrgpstat = GPOS_NEW(pmp) DrgPstats(pmp);
	const ULONG ulRelStat = pdrgpdxlstatsderrel->UlLength();
	for (ULONG ulIdxRelStat = 0; ulIdxRelStat < ulRelStat; ulIdxRelStat++)
	{
		// create hash map from colid -> histogram
		HMUlHist *phmulhist = GPOS_NEW(pmp) HMUlHist(pmp);
		
		// width hash map
		HMUlDouble *phmuldouble = GPOS_NEW(pmp) HMUlDouble(pmp);
		
		CDXLStatsDerivedRelation *pdxlstatsderrel = (*pdrgpdxlstatsderrel)[ulIdxRelStat];
		const DrgPdxlstatsdercol *pdrgpdxlstatsdercol = pdxlstatsderrel->Pdrgpdxlstatsdercol();
		
		const ULONG ulColStats = pdrgpdxlstatsdercol->UlLength();
		for (ULONG ulIdxColStat = 0; ulIdxColStat < ulColStats; ulIdxColStat++)
		{
			CDXLStatsDerivedColumn *pdxlstatsdercol = (*pdrgpdxlstatsdercol)[ulIdxColStat];
			
			ULONG ulColId = pdxlstatsdercol->UlColId();
			CDouble dWidth = pdxlstatsdercol->DWidth();
			CDouble dNullFreq = pdxlstatsdercol->DNullFreq();
			CDouble dDistinctRemain = pdxlstatsdercol->DDistinctRemain();
			CDouble dFreqRemain = pdxlstatsdercol->DFreqRemain();
			
			DrgPbucket *pdrgppbucket = CDXLUtils::Pdrgpbucket(pmp, pmda, pdxlstatsdercol);
			CHistogram *phist = GPOS_NEW(pmp) CHistogram(pdrgppbucket, true /*fWellDefined*/, dNullFreq, dDistinctRemain, dFreqRemain);
			
			phmulhist->FInsert(GPOS_NEW(pmp) ULONG(ulColId), phist);
			phmuldouble->FInsert(GPOS_NEW(pmp) ULONG(ulColId), GPOS_NEW(pmp) CDouble(dWidth));
		}
		
		CDouble dRows = pdxlstatsderrel->DRows();
		CStatistics *pstats = GPOS_NEW(pmp) CStatistics
										(
										pmp,
										phmulhist,
										phmuldouble,
										dRows,
										false /* fEmpty */
										);
		//pstats->AddCardUpperBound(pmp, ulIdxRelStat, dRows);

		pdrgpstat->Append(pstats);
	}
	
	return pdrgpstat;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::Pdrgpbucket
//
//	@doc:
//		Extract the array of optimizer buckets from the dxl representation of
//		dxl buckets in the dxl derived column statistics object.
//---------------------------------------------------------------------------
DrgPbucket *
CDXLUtils::Pdrgpbucket
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	CDXLStatsDerivedColumn *pdxlstatsdercol
	)
{
	DrgPbucket *pdrgppbucket = GPOS_NEW(pmp) DrgPbucket(pmp);
	
	const DrgPdxlbucket *pdrgpdxlbucket = pdxlstatsdercol->Pdrgpdxlbucket();	
	const ULONG ulBuckets = pdrgpdxlbucket->UlLength();
	for (ULONG ul = 0; ul < ulBuckets; ul++)
	{
		CDXLBucket *pdxlbucket = (*pdrgpdxlbucket)[ul];
		
		// translate the lower and upper bounds of the bucket
		IDatum *pdatumLower = Pdatum(pmp, pmda, pdxlbucket->PdxldatumLower());
		CPoint *ppointLower = GPOS_NEW(pmp) CPoint(pdatumLower);
		
		IDatum *pdatumUpper = Pdatum(pmp, pmda, pdxlbucket->PdxldatumUpper());
		CPoint *ppointUpper = GPOS_NEW(pmp) CPoint(pdatumUpper);
		
		CBucket *pbucket = GPOS_NEW(pmp) CBucket
										(
										ppointLower,
										ppointUpper,
										pdxlbucket->FLowerClosed(),
										pdxlbucket->FUpperClosed(),
										pdxlbucket->DFrequency(),
										pdxlbucket->DDistinct()
										);
		
		pdrgppbucket->Append(pbucket);
	}
	
	return pdrgppbucket;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::Pdatum
//
//	@doc:
//		Translate the optimizer datum from dxl datum object
//
//---------------------------------------------------------------------------	
IDatum *
CDXLUtils::Pdatum
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	const CDXLDatum *pdxldatum
	 )
{
	IMDId *pmdid = pdxldatum->Pmdid();
	return pmda->Pmdtype(pmdid)->Pdatum(pmp, pdxldatum);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::PdrgpmdobjParseDXL
//
//	@doc:
//		Parse a list of metadata objects from the given DXL string.
//		If a non-empty XSD schema location is provided, the DXL is validated against
//		that schema, and an exception is thrown if the DXL does not conform.
//
//---------------------------------------------------------------------------
DrgPimdobj *
CDXLUtils::PdrgpmdobjParseDXL
	(
	IMemoryPool *pmp,
	const CWStringBase *pstr,
	const CHAR *szXSDPath
	)
{
	GPOS_ASSERT(NULL != pmp);

	// create and install a parse handler for the DXL document
	CParseHandlerDXL *pphdxl = PphdxlParseDXL(pmp, pstr, szXSDPath);
	CAutoP<CParseHandlerDXL> a_pphdxl(pphdxl);
	
	// collect metadata objects from dxl parse handler
	DrgPimdobj *pdrgpmdobj = pphdxl->Pdrgpmdobj();
	pdrgpmdobj->AddRef();
	
	return pdrgpmdobj;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::PimdobjParseDXL
//
//	@doc:
//		Parse a single metadata object given its DXL representation.
// 		Returns NULL if the DXL represents no metadata objects, or the first parsed
//		object if it does.
//		If a non-empty XSD schema location is provided, the DXL is validated against
//		that schema, and an exception is thrown if the DXL does not conform.
//
//---------------------------------------------------------------------------
IMDCacheObject *
CDXLUtils::PimdobjParseDXL
	(
	IMemoryPool *pmp,
	const CWStringBase *pstr,
	const CHAR *szXSDPath
	)
{
	GPOS_ASSERT(NULL != pmp);

	// create and install a parse handler for the DXL document
	CAutoP<CParseHandlerDXL> a_pphdxl(PphdxlParseDXL(pmp, pstr, szXSDPath));
	
	// collect metadata objects from dxl parse handler
	DrgPimdobj *pdrgpmdobj = a_pphdxl->Pdrgpmdobj();

	if (0 == pdrgpmdobj->UlLength())
	{
		// no metadata objects found
		return NULL;
	}

	IMDCacheObject *pimdobjResult = (*pdrgpmdobj)[0];
	pimdobjResult->AddRef();
	
	return pimdobjResult;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::ValidateDXL
//
//	@doc:
//		Validate DXL document against an XSD schema.
//		Throws an exception if the document is invalid.
//
//---------------------------------------------------------------------------
void
CDXLUtils::ValidateDXL
	(
	IMemoryPool *pmp,
	const CHAR *szDXL,
	const CHAR *szXSDPath				
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != szDXL);
	GPOS_ASSERT(NULL != szXSDPath);
	
	GPOS_CHECK_ABORT;

	// setup own memory manager
	CDXLMemoryManager *pmm = GPOS_NEW(pmp) CDXLMemoryManager(pmp);
	SAX2XMLReader* pxmlreader = NULL;
	{
		// we need to disable OOM simulation here, otherwise xerces throws ABORT signal
		CAutoTraceFlag atf(EtraceSimulateOOM, false);
		CAutoTraceFlag atf2(EtraceSimulateAbort, false);
		pxmlreader = XMLReaderFactory::createXMLReader(pmm);
		GPOS_CHECK_ABORT;
	}
	
	// setup XSD validation
	pxmlreader->setFeature(XMLUni::fgSAX2CoreValidation, true);
	pxmlreader->setFeature(XMLUni::fgXercesDynamic, false);
	pxmlreader->setFeature(XMLUni::fgSAX2CoreNameSpaces, true);
	pxmlreader->setFeature(XMLUni::fgXercesSchema, true);
	
	pxmlreader->setFeature(XMLUni::fgXercesSchemaFullChecking, true);
	pxmlreader->setFeature(XMLUni::fgSAX2CoreNameSpacePrefixes, true);
	
	{
		CAutoTraceFlag atf(EtraceSimulateOOM, false);

		CAutoP<XMLCh> a_xmlszXSDPath;
		a_xmlszXSDPath = XMLString::transcode(szXSDPath, pmm);
		pxmlreader->setProperty(XMLUni::fgXercesSchemaExternalSchemaLocation, (void*) a_xmlszXSDPath.Pt());
	}
	
	CParseHandlerDummy phdummy(pmm);
	pxmlreader->setContentHandler(&phdummy);
	pxmlreader->setErrorHandler(&phdummy);
		
	MemBufInputSource *pmbis = NULL;
	{
		CAutoTraceFlag atf(EtraceSimulateOOM, false);

		pmbis = new(pmm) MemBufInputSource(
				(const XMLByte*) szDXL,
				strlen(szDXL),
				"dxl validation",
				false,
				pmm
	    	);
	}
	
	GPOS_CHECK_ABORT;
	try
	{
		CAutoTraceFlag atf(EtraceSimulateOOM, false);
		CAutoTraceFlag atf2(EtraceSimulateAbort, false);
		pxmlreader->parse(*pmbis);
		GPOS_CHECK_ABORT;
	}
	catch (const XMLException&)
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLXercesParseError);
	}
	catch (const SAXParseException&)
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLXercesParseError);
	}
	catch (const SAXException&)
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLXercesParseError);
	}

	GPOS_CHECK_ABORT;

	// cleanup
	GPOS_DELETE(pmm);
	delete pxmlreader;
	delete pmbis;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::SerializeQuery
//
//	@doc:
//		Serialize a DXL Query tree into a DXL document
//
//---------------------------------------------------------------------------
void
CDXLUtils::SerializeQuery
	(
	IMemoryPool *pmp,
	IOstream &os,
	const CDXLNode *pdxlnQuery,
	const DrgPdxln *pdrgpdxlnQueryOutput,
	const DrgPdxln *pdrgpdxlnCTE,
	BOOL fSerializeHeaderFooter,
	BOOL fIndent
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pdxlnQuery && NULL != pdrgpdxlnQueryOutput);

	CAutoTimer at("\n[OPT]: DXL Query Serialization Time", GPOS_FTRACE(EopttracePrintOptimizationStatistics));

	CXMLSerializer xmlser(pmp, os, fIndent);

	if (fSerializeHeaderFooter)
	{
		SerializeHeader(pmp, &xmlser);
	}
	
	xmlser.OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenQuery));

	// serialize the query output columns
	xmlser.OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenQueryOutput));
	for (ULONG ul = 0; ul < pdrgpdxlnQueryOutput->UlLength(); ++ul)
	{
		CDXLNode *pdxlnScId = (*pdrgpdxlnQueryOutput)[ul];
		pdxlnScId->SerializeToDXL(&xmlser);
	}
	xmlser.CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenQueryOutput));

	// serialize the CTE list
	xmlser.OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenCTEList));
	const ULONG ulCTEs = pdrgpdxlnCTE->UlLength();
	for (ULONG ul = 0; ul < ulCTEs; ++ul)
	{
		CDXLNode *pdxlnCTE = (*pdrgpdxlnCTE)[ul];
		pdxlnCTE->SerializeToDXL(&xmlser);
	}
	xmlser.CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenCTEList));

	
	pdxlnQuery->SerializeToDXL(&xmlser);

	xmlser.CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenQuery));
	
	if (fSerializeHeaderFooter)
	{
		SerializeFooter(&xmlser);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::PstrSerializeULLONG
//
//	@doc:
//		Serialize a ULLONG value
//
//---------------------------------------------------------------------------
CWStringDynamic *
CDXLUtils::PstrSerializeULLONG
	(
	IMemoryPool *pmp,
	ULLONG ullVal
	)
{
	GPOS_ASSERT(NULL != pmp);
	CAutoTraceFlag atf(EtraceSimulateAbort, false);

	CAutoP<CWStringDynamic> a_pstr(GPOS_NEW(pmp) CWStringDynamic(pmp));

	// create a string stream to hold the result of serialization
	COstreamString oss(a_pstr.Pt());
	oss << ullVal;

	return a_pstr.PtReset();
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::SerializePlan
//
//	@doc:
//		Serialize a DXL tree into a DXL document
//
//---------------------------------------------------------------------------
void
CDXLUtils::SerializePlan
	(
	IMemoryPool *pmp,
	IOstream& os,
	const CDXLNode *pdxln,
	ULLONG ullPlanId,
	ULLONG ullPlanSpaceSize,
	BOOL fSerializeHeaderFooter,
	BOOL fIndent
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pdxln);

	CAutoTimer at("\n[OPT]: DXL Plan Serialization Time", GPOS_FTRACE(EopttracePrintOptimizationStatistics));

	CXMLSerializer xmlser(pmp, os, fIndent);
	
	if (fSerializeHeaderFooter)
	{
		SerializeHeader(pmp, &xmlser);
	}
	
	xmlser.OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenPlan));

	// serialize plan id and space size attributes

	xmlser.AddAttribute(CDXLTokens::PstrToken(EdxltokenPlanId), ullPlanId);
	xmlser.AddAttribute(CDXLTokens::PstrToken(EdxltokenPlanSpaceSize), ullPlanSpaceSize);

	pdxln->SerializeToDXL(&xmlser);

	xmlser.CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenPlan));
	
	if (fSerializeHeaderFooter)
	{
		SerializeFooter(&xmlser);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::SerializeMetadata
//
//	@doc:
//		Serialize a list of MD objects into a DXL document and write to
//		to the provided output stream
//
//---------------------------------------------------------------------------
void
CDXLUtils::SerializeMetadata
	(
	IMemoryPool *pmp,
	const DrgPimdobj *pdrgpmdobj,
	IOstream &os,
	BOOL fSerializeHeaderFooter,
	BOOL fIndent
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pdrgpmdobj);

	CXMLSerializer xmlser(pmp, os, fIndent);

	if (fSerializeHeaderFooter)
	{
		SerializeHeader(pmp, &xmlser);
	}
	
	xmlser.OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenMetadata));


	for (ULONG ul = 0; ul < pdrgpmdobj->UlLength(); ul++)
	{
		IMDCacheObject *pimdobj = (*pdrgpmdobj)[ul];
		pimdobj->Serialize(&xmlser);
	}

	xmlser.CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenMetadata));

	if (fSerializeHeaderFooter)
	{
		SerializeFooter(&xmlser);
	}
	
	return;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::PstrSerializeMetadata
//
//	@doc:
//		Serialize a list of MD objects into a DXL document and write to
//		to the provided output stream
//
//---------------------------------------------------------------------------
CWStringDynamic *
CDXLUtils::PstrSerializeMetadata
	(
	IMemoryPool *pmp,
	const IMDId *pmdid,
	BOOL fSerializeHeaderFooter,
	BOOL fIndent
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(pmdid->FValid());

	CWStringDynamic *pstr = GPOS_NEW(pmp) CWStringDynamic(pmp);
	
	// create a string stream to hold the result of serialization
	COstreamString oss(pstr);
	
	CXMLSerializer xmlser(pmp, oss, fIndent);

	if (fSerializeHeaderFooter)
	{
		SerializeHeader(pmp, &xmlser);
	}
	
	xmlser.OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenMetadata));
	
	xmlser.OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), 
									CDXLTokens::PstrToken(EdxltokenMdid));				
	pmdid->Serialize(&xmlser, CDXLTokens::PstrToken(EdxltokenValue));
	xmlser.CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), 
					CDXLTokens::PstrToken(EdxltokenMdid));
	
	xmlser.CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenMetadata));

	if (fSerializeHeaderFooter)
	{
		SerializeFooter(&xmlser);
	}
	
	return pstr;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::PstrSerializeSamplePlans
//
//	@doc:
//		Serialize a list of sample plans in the given enumerator config
//
//---------------------------------------------------------------------------
CWStringDynamic *
CDXLUtils::PstrSerializeSamplePlans
	(
	IMemoryPool *pmp,
	CEnumeratorConfig *pec,
	BOOL fIndent
	)
{
	GPOS_ASSERT(NULL != pmp);

	CWStringDynamic *pstr = GPOS_NEW(pmp) CWStringDynamic(pmp);

	// create a string stream to hold the result of serialization
	COstreamString oss(pstr);

	CXMLSerializer xmlser(pmp, oss, fIndent);
	SerializeHeader(pmp, &xmlser);

	xmlser.OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenSamplePlans));

	const ULONG ulSize = pec->UlCreatedSamples();
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		xmlser.OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenSamplePlan));
		// we add 1 to plan id since id's are zero-based internally, and we reserve 0 for best plan
		xmlser.AddAttribute(CDXLTokens::PstrToken(EdxltokenPlanId), pec->UllPlanSample(ul) + 1);
		xmlser.AddAttribute(CDXLTokens::PstrToken(EdxltokenRelativeCost), pec->CostPlanSample(ul) / pec->CostBest());
		xmlser.CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenSamplePlan));
	}

	xmlser.CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenSamplePlans));

	SerializeFooter(&xmlser);

	return pstr;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::PstrSerializeCostDistr
//
//	@doc:
//		Serialize cost distribution
//
//---------------------------------------------------------------------------
CWStringDynamic *
CDXLUtils::PstrSerializeCostDistr
	(
	IMemoryPool *pmp,
	CEnumeratorConfig *pec,
	BOOL fIndent
	)
{
	GPOS_ASSERT(NULL != pmp);

	CWStringDynamic *pstr = GPOS_NEW(pmp) CWStringDynamic(pmp);

	// create a string stream to hold the result of serialization
	COstreamString oss(pstr);

	CXMLSerializer xmlser(pmp, oss, fIndent);
	SerializeHeader(pmp, &xmlser);

	xmlser.OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenCostDistr));

	const ULONG ulSize = pec->UlCostDistrSize();
	for (ULLONG ul = 0; ul < ulSize; ul++)
	{
		xmlser.OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenValue));
		xmlser.AddAttribute(CDXLTokens::PstrToken(EdxltokenX), pec->DCostDistrX(ul));
		xmlser.AddAttribute(CDXLTokens::PstrToken(EdxltokenY), pec->DCostDistrY(ul));
		xmlser.CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenValue));
	}

	xmlser.CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenCostDistr));

	SerializeFooter(&xmlser);

	return pstr;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::SerializeMDRequest
//
//	@doc:
//		Serialize a list of mdids into a DXL MD Request document and write to
//		to the provided output stream
//
//---------------------------------------------------------------------------
void
CDXLUtils::SerializeMDRequest
	(
	IMemoryPool *pmp,
	CMDRequest *pmdr,
	IOstream &os,
	BOOL fSerializeHeaderFooter,
	BOOL fIndent
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pmdr);

	CXMLSerializer xmlser(pmp, os, fIndent);

	if (fSerializeHeaderFooter)
	{
		SerializeHeader(pmp, &xmlser);
	}

	pmdr->Serialize(&xmlser);

	if (fSerializeHeaderFooter)
	{
		SerializeFooter(&xmlser);
	}

	return;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::SerializeMDRequest
//
//	@doc:
//		Serialize a list of mdids into a DXL MD Request document and write to
//		to the provided output stream
//
//---------------------------------------------------------------------------
void
CDXLUtils::SerializeMDRequest
	(
	IMemoryPool *pmp,
	const IMDId *pmdid,
	IOstream &os,
	BOOL fSerializeHeaderFooter,
	BOOL fIndent
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pmdid);

	CXMLSerializer xmlser(pmp, os, fIndent);

	if (fSerializeHeaderFooter)
	{
		SerializeHeader(pmp, &xmlser);
	}

	xmlser.OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenMDRequest));

	xmlser.OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenMdid));
	pmdid->Serialize(&xmlser, CDXLTokens::PstrToken(EdxltokenValue));
	xmlser.CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenMdid));

	xmlser.CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenMDRequest));

	if (fSerializeHeaderFooter)
	{
		SerializeFooter(&xmlser);
	}

	return;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::PstrSerializeStatistics
//
//	@doc:
//		Serialize a list of statistics objects into a DXL document
//
//---------------------------------------------------------------------------
CWStringDynamic *
CDXLUtils::PstrSerializeStatistics
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda, 
	const DrgPstats *pdrgpstat,
	BOOL fSerializeHeaderFooter,
	BOOL fIndent
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pdrgpstat);
	CWStringDynamic *pstr = GPOS_NEW(pmp) CWStringDynamic(pmp);

	// create a string stream to hold the result of serialization
	COstreamString oss(pstr);

	CDXLUtils::SerializeStatistics(pmp, pmda, pdrgpstat, oss, fSerializeHeaderFooter, fIndent);

	return pstr;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::PstrSerializeStatistics
//
//	@doc:
//		Serialize a list of statistics objects into a DXL document and write to
//		to the provided output stream
//
//---------------------------------------------------------------------------
void
CDXLUtils::SerializeStatistics
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	const DrgPstats *pdrgpstat,
	IOstream &os,
	BOOL fSerializeHeaderFooter,
	BOOL fIndent
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pdrgpstat);

	CXMLSerializer xmlser(pmp, os, fIndent);

	if (fSerializeHeaderFooter)
	{
		SerializeHeader(pmp, &xmlser);
	}

	xmlser.OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenStatistics));

	GPOS_ASSERT(NULL != pdrgpstat);

	for (ULONG ul = 0; ul < pdrgpstat->UlLength(); ul++)
	{
		CStatistics *pstats = (*pdrgpstat)[ul];
		CDXLStatsDerivedRelation *pdxlstatsderrel = pstats->Pdxlstatsderrel(pmp, pmda);
		pdxlstatsderrel->Serialize(&xmlser);
		pdxlstatsderrel->Release();
	}

	xmlser.CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenStatistics));

	if (fSerializeHeaderFooter)
	{
		SerializeFooter(&xmlser);
	}

	return;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::PstrSerializeMetadata
//
//	@doc:
//		Serialize a list of MD objects into a DXL document
//
//---------------------------------------------------------------------------
CWStringDynamic *
CDXLUtils::PstrSerializeMetadata
	(
	IMemoryPool *pmp,
	const DrgPimdobj *pdrgpmdobj,
	BOOL fSerializeHeaderFooter,
	BOOL fIndent
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pdrgpmdobj);
	CWStringDynamic *pstr = GPOS_NEW(pmp) CWStringDynamic(pmp);
	
	// create a string stream to hold the result of serialization
	COstreamString oss(pstr);
	
	CDXLUtils::SerializeMetadata(pmp, pdrgpmdobj, oss, fSerializeHeaderFooter, fIndent);

	return pstr;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::PstrSerializeMDObj
//
//	@doc:
//		Serialize an MD object into a DXL document
//
//---------------------------------------------------------------------------
CWStringDynamic *
CDXLUtils::PstrSerializeMDObj
	(
	IMemoryPool *pmp,
	const IMDCacheObject *pimdobj,
	BOOL fSerializeHeaderFooter,
	BOOL fIndent
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pimdobj);
	CAutoTraceFlag atf2(EtraceSimulateAbort, false);

	CAutoP<CWStringDynamic> a_pstr(GPOS_NEW(pmp) CWStringDynamic(pmp));
	
	// create a string stream to hold the result of serialization
	COstreamString oss(a_pstr.Pt());

	CXMLSerializer xmlser(pmp, oss, fIndent);
	
	if (fSerializeHeaderFooter)
	{
		SerializeHeader(pmp, &xmlser);
		xmlser.OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenMetadata));
	}
	GPOS_CHECK_ABORT;
	
	pimdobj->Serialize(&xmlser);
	GPOS_CHECK_ABORT;
		
	if (fSerializeHeaderFooter)
	{
		xmlser.CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenMetadata));
		SerializeFooter(&xmlser);
	}
	
	GPOS_CHECK_ABORT;
	return a_pstr.PtReset();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::PstrSerializeScalarExpr
//
//	@doc:
//		Serialize a DXL tree representing a ScalarExpr into a DXL document
//
//---------------------------------------------------------------------------
CWStringDynamic *
CDXLUtils::PstrSerializeScalarExpr
	(
	IMemoryPool *pmp,
	const CDXLNode *pdxln,
	BOOL fSerializeHeaderFooter,
	BOOL fIndent
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pdxln);
	CWStringDynamic *pstr = GPOS_NEW(pmp) CWStringDynamic(pmp);

	// create a string stream to hold the result of serialization
	COstreamString oss(pstr);
	CXMLSerializer xmlser(pmp, oss, fIndent);

	if (fSerializeHeaderFooter)
	{
		SerializeHeader(pmp, &xmlser);
	}
	xmlser.OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenScalarExpr));

	// serialize the content of the scalar expression
	pdxln->SerializeToDXL(&xmlser);
	xmlser.CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenScalarExpr));
	if (fSerializeHeaderFooter)
	{
		SerializeFooter(&xmlser);
	}
	return pstr;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::SerializeHeader
//
//	@doc:
//		Serialize the DXL document header
//
//---------------------------------------------------------------------------
void
CDXLUtils::SerializeHeader
	(
	IMemoryPool *pmp,
	CXMLSerializer *pxmlser
	)
{
	GPOS_ASSERT(NULL != pxmlser);
	
	pxmlser->StartDocument();
	
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenDXLMessage));
	
	// add namespace specification xmlns:dxl="...."
	CWStringDynamic dstrNamespaceAttr(pmp);
	dstrNamespaceAttr.AppendFormat
						(
						GPOS_WSZ_LIT("%ls%ls%ls"),
						CDXLTokens::PstrToken(EdxltokenNamespaceAttr)->Wsz(),
						CDXLTokens::PstrToken(EdxltokenColon)->Wsz(),
						CDXLTokens::PstrToken(EdxltokenNamespacePrefix)->Wsz()
						);
	
	pxmlser->AddAttribute(&dstrNamespaceAttr, CDXLTokens::PstrToken(EdxltokenNamespaceURI));
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::SerializeFooter
//
//	@doc:
//		Serialize the DXL document footer
//
//---------------------------------------------------------------------------
void
CDXLUtils::SerializeFooter
	(
	CXMLSerializer *pxmlser
	)
{
	GPOS_ASSERT(NULL != pxmlser);
	
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenDXLMessage));
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::PstrFromXMLCh
//
//	@doc:
//		Create a GPOS string object from a Xerces XMLCh* string.
//		The function allocates the returned string in the provided memory pool
//		and it is the responsibility of the caller to release it.
//
//---------------------------------------------------------------------------
CWStringDynamic *
CDXLUtils::PstrFromXMLCh
	(
	CDXLMemoryManager *pmm,
	const XMLCh *xmlsz
	)
{
	GPOS_ASSERT(NULL != pmm);
	GPOS_ASSERT(NULL != xmlsz);
	
	IMemoryPool *pmp = pmm->Pmp();
	
	{
		CAutoTraceFlag atf(EtraceSimulateOOM, false);
		CHAR *sz = XMLString::transcode(xmlsz, pmm);

		CWStringDynamic *pstr = GPOS_NEW(pmp) CWStringDynamic(pmp);
		pstr->AppendFormat(GPOS_WSZ_LIT("%s"), sz);
	
		// cleanup temporary buffer
		XMLString::release(&sz, pmm);
	
		return pstr;
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::PstrFromBase64XMLStr
//
//	@doc:
//		Create a decoded byte array from a base 64 encoded XML string.
//
//---------------------------------------------------------------------------
BYTE *
CDXLUtils::PbaFromBase64XMLStr
	(
	CDXLMemoryManager *pmm,
	const XMLCh *xmlsz,
	ULONG *pulLength // output: length of constructed byte array
	)
{
	GPOS_ASSERT(NULL != pmm);
	GPOS_ASSERT(NULL != xmlsz);

	CAutoTraceFlag atf(EtraceSimulateOOM, false);
	IMemoryPool *pmp = pmm->Pmp();

	// find out xml string length
	ULONG ulLen = XMLString::stringLen(xmlsz);

	// convert XML string into array of XMLByte
	CAutoRg<XMLByte> a_dataInByte;
	a_dataInByte = (XMLByte *) GPOS_NEW_ARRAY(pmp, XMLByte, ulLen + 1);
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		GPOS_ASSERT(xmlsz[ul] <= 256 && "XML string not in Base64 encoding");

		a_dataInByte[ul] = (XMLByte) xmlsz[ul];
	}
	a_dataInByte[ulLen] = 0;

	// decode string
	XMLSize_t xmlBASize = 0;
	XMLByte *pxmlbyteOut = Base64::decode(a_dataInByte.Rgt(), &xmlBASize, pmm);
	*pulLength = static_cast<ULONG>(xmlBASize);

	return static_cast<BYTE *>(pxmlbyteOut);
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::PstrFromSz
//
//	@doc:
//		Create a GPOS string object from a character array.
//		The function allocates the returned string in the provided memory pool
//		and it is the responsibility of the caller to release it.
//
//---------------------------------------------------------------------------
CWStringDynamic *
CDXLUtils::PstrFromSz
	(
	IMemoryPool *pmp,
	const CHAR *sz
	)
{
	GPOS_ASSERT(NULL != sz);
	
	CAutoP<CWStringDynamic> a_pstr(GPOS_NEW(pmp) CWStringDynamic(pmp));
	a_pstr->AppendFormat(GPOS_WSZ_LIT("%s"), sz);
	return a_pstr.PtReset();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::PmdnameFromSz
//
//	@doc:
//		Create a GPOS string object from a character array.
//		The function allocates the returned string in the provided memory pool
//		and it is the responsibility of the caller to release it.
//
//---------------------------------------------------------------------------
CMDName *
CDXLUtils::PmdnameFromSz
	(
	IMemoryPool *pmp,
	const CHAR *sz
	)
{
	GPOS_ASSERT(NULL != sz);
	
	CWStringDynamic *pstr = CDXLUtils::PstrFromSz(pmp, sz);
	CMDName *pmdname = GPOS_NEW(pmp) CMDName(pmp, pstr);

	// CMDName ctor created a copy of the string
	GPOS_DELETE(pstr);
	
	return pmdname;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::PmdnameFromXmlsz
//
//	@doc:
//		Create a GPOS string object from a Xerces character array.
//		The function allocates the returned string in the provided memory pool
//		and it is the responsibility of the caller to release it.
//
//---------------------------------------------------------------------------
CMDName *
CDXLUtils::PmdnameFromXmlsz
	(
	CDXLMemoryManager *pmm,
	const XMLCh *xmlsz
	)
{
	GPOS_ASSERT(NULL != xmlsz);
	
	CHAR *sz = XMLString::transcode(xmlsz, pmm);
	CMDName *pmdname = PmdnameFromSz(pmm->Pmp(), sz);
	
	// cleanup temporary buffer
	XMLString::release(&sz, pmm);
	
	return pmdname;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::PstrFromByteArray
//
//	@doc:
//		Use Base64 encoding to convert bytearray to a string.
//
//---------------------------------------------------------------------------
CWStringDynamic *
CDXLUtils::PstrFromByteArray
	(
	IMemoryPool *pmp,
	const BYTE *pba,
	ULONG ulLength
	)
{
	CAutoP<CDXLMemoryManager> a_pmm(GPOS_NEW(pmp) CDXLMemoryManager(pmp));
	CAutoP<CWStringDynamic> a_pstr(GPOS_NEW(pmp) CWStringDynamic(pmp));

	GPOS_ASSERT(ulLength > 0);

	XMLSize_t outputLength = 0;
	const XMLByte *input = (const XMLByte *) pba;

	XMLSize_t inputLength = (XMLSize_t) ulLength;

	CAutoRg<XMLByte> a_pxmlbyteBuf;

	{
		CAutoTraceFlag atf(EtraceSimulateOOM, false);
		a_pxmlbyteBuf = Base64::encode(input, inputLength, &outputLength, a_pmm.Pt());
	}

	GPOS_ASSERT(NULL != a_pxmlbyteBuf.Rgt());

	// assert that last byte is 0
	GPOS_ASSERT(0 == a_pxmlbyteBuf[outputLength]);

	// there may be padded bytes. We don't need them. We zero out there bytes.
#ifdef GPOS_DEBUG
	ULONG ulNewLength = outputLength;
#endif  // GPOS_DEBUG
	while (('\n' == a_pxmlbyteBuf[outputLength]
			|| 0 == a_pxmlbyteBuf[outputLength])
			&& 0 < outputLength)
	{
		a_pxmlbyteBuf[outputLength] = 0;
#ifdef GPOS_DEBUG
		ulNewLength = outputLength;
#endif  // GPOS_DEBUG
		outputLength--;
	}
	GPOS_ASSERT(0 == a_pxmlbyteBuf[ulNewLength]);

	CHAR *szRetBuf = (CHAR *) (a_pxmlbyteBuf.Rgt());
	a_pstr->AppendCharArray(szRetBuf);

	return a_pstr.PtReset();
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::PByteArrayFromStr
//
//	@doc:
//		Decode byte array from Base64 encoded string.
//
//---------------------------------------------------------------------------
BYTE *
CDXLUtils::PByteArrayFromStr
	(
	IMemoryPool *pmp,
	const CWStringDynamic *pstr,
	ULONG *ulLength
	)
{
	CAutoP<CDXLMemoryManager> a_pmm(GPOS_NEW(pmp) CDXLMemoryManager(pmp));

	XMLSize_t xmlBASize = 0;

	const WCHAR *pwc = pstr->Wsz();

	// We know that the input is encoded using Base64.
	XMLSize_t srcLen = pstr->UlLength();

	CAutoRg<XMLByte> a_dataInByte;

	a_dataInByte = (XMLByte*) GPOS_NEW_ARRAY(pmp, XMLByte, srcLen+1);

	for (XMLSize_t i = 0; i < srcLen; i++)
	{
		GPOS_ASSERT(pwc[i] <= 256);
		a_dataInByte[i] = (XMLByte) pwc[i];
	}

	a_dataInByte[srcLen] = 0;

	XMLByte *pxmlba = NULL;
	{
		CAutoTraceFlag atf(EtraceSimulateOOM, false);
		pxmlba = Base64::decode(a_dataInByte.Rgt(), &xmlBASize, a_pmm.Pt());
	}

	(* ulLength) = static_cast<ULONG>(xmlBASize);

	return static_cast<BYTE *>(pxmlba);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::PstrSerialize
//
//	@doc:
//		Serialize a list of unsigned integers into a comma-separated string
//
//---------------------------------------------------------------------------
CWStringDynamic *
CDXLUtils::PstrSerialize
	(
	IMemoryPool *pmp,
	const DrgPdrgPul *pdrgpdrgpul
	)
{
	const ULONG ulLen = pdrgpdrgpul->UlLength();
	CWStringDynamic *pstrKeys = GPOS_NEW(pmp) CWStringDynamic(pmp);
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		DrgPul *pdrgpul = (*pdrgpdrgpul)[ul];
		CWStringDynamic *pstrKeySet = CDXLUtils::PstrSerialize(pmp, pdrgpul);

		pstrKeys->Append(pstrKeySet);

		if (ul < ulLen - 1)
		{
			pstrKeys->AppendFormat(GPOS_WSZ_LIT("%ls"), GPOS_WSZ_LIT(";"));
		}

		GPOS_DELETE(pstrKeySet);
	}

	return pstrKeys;
}

// Serialize a list of chars into a comma-separated string
CWStringDynamic *
CDXLUtils::PstrSerializeSz
	(
	IMemoryPool *pmp,
	const DrgPsz *pdrgsz
	)
{
	CWStringDynamic *pstr = GPOS_NEW(pmp) CWStringDynamic(pmp);

	ULONG ulLength = pdrgsz->UlLength();
	for (ULONG ul = 0; ul < ulLength; ul++)
	{
		CHAR tValue = *((*pdrgsz)[ul]);
		if (ul == ulLength - 1)
		{
			// last element: do not print a comma
			pstr->AppendFormat(GPOS_WSZ_LIT("%c"), tValue);
		}
		else
		{
			pstr->AppendFormat(GPOS_WSZ_LIT("%c%ls"), tValue, CDXLTokens::PstrToken(EdxltokenComma)->Wsz());
		}
	}

	return pstr;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::SzFromWsz
//
//	@doc:
//		Converts a wide character string into a character array in the provided memory pool
//
//---------------------------------------------------------------------------
CHAR *
CDXLUtils::SzFromWsz
	(
	IMemoryPool *pmp,
	const WCHAR *wsz
	)
{
	GPOS_ASSERT(NULL != wsz);

	ULONG ulMaxLength = GPOS_WSZ_LENGTH(wsz) * GPOS_SIZEOF(WCHAR) + 1;
	CHAR *sz = GPOS_NEW_ARRAY(pmp, CHAR, ulMaxLength);
	CAutoRg<CHAR> a_sz(sz);

#ifdef GPOS_DEBUG
	INT i = (INT)
#endif
	wcstombs(sz, wsz, ulMaxLength);
	GPOS_ASSERT(0 <= i);

	sz[ulMaxLength - 1] = '\0';

	return a_sz.RgtReset();
}

//---------------------------------------------------------------------------
//		CDXLUtils::SzRead
//
//	@doc:
//		Read a given text file in a character buffer.
//		The function allocates memory from the provided memory pool, and it is
//		the responsibility of the caller to deallocate it.
//
//---------------------------------------------------------------------------
CHAR *
CDXLUtils::SzRead
	(
	IMemoryPool *pmp,
	const CHAR *szFileName
	)
{
	GPOS_TRACE_FORMAT("opening file %s", szFileName);

	CFileReader fr;
	fr.Open(szFileName);

	ULONG_PTR ulpFileSize = (ULONG_PTR) fr.UllSize();
	CAutoRg<CHAR> a_szBuffer(GPOS_NEW_ARRAY(pmp, CHAR, ulpFileSize + 1));
	
	ULONG_PTR ulpRead = fr.UlpRead((BYTE *) a_szBuffer.Rgt(), ulpFileSize);
	fr.Close();
	
	GPOS_ASSERT(ulpRead == ulpFileSize);
	
	a_szBuffer[ulpRead] = '\0';
		
	return a_szBuffer.RgtReset();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::SerializeBound
//
//	@doc:
//		Serialize a datum with a given tag. Result xml looks like
// 		<tag>
//			<const.../>
//		</tag>
//
//---------------------------------------------------------------------------
void
CDXLUtils::SerializeBound
	(
	IDatum *pdatum,
	const CWStringConst *pstr,
	CXMLSerializer *pxmlser
	)
{
	pxmlser->OpenElement
				(
				CDXLTokens::PstrToken(EdxltokenNamespacePrefix),
				pstr
				);

	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDType *pmdtype = pmda->Pmdtype(pdatum->Pmdid());
	CDXLScalarConstValue *pdxlop = pmdtype->PdxlopScConst(pxmlser->Pmp(), pdatum);
	pdxlop->SerializeToDXL(pxmlser, NULL);

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix),
						pstr);
	pdxlop->Release();
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::DebugPrintDrgpmdid
//
//	@doc:
//		Print an array of mdids
//
//---------------------------------------------------------------------------
void
CDXLUtils::DebugPrintDrgpmdid
	(
	IOstream &os,
	DrgPmdid *pdrgpmdid
	)
{
	ULONG ulLen = pdrgpmdid->UlLength();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		const IMDId *pmdid = (*pdrgpmdid)[ul];
		pmdid->OsPrint(os);
		os << " ";
	}

	os << std::endl;
}
#endif

// EOF

