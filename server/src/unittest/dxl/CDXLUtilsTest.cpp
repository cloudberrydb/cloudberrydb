//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CDXLUtilsTest.cpp
//
//	@doc:
//		Tests DXL utility functions
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/io/COstreamString.h"
#include "gpos/error/CAutoTrace.h"
#include "gpos/common/CRandom.h"
#include "gpos/common/CAutoP.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/test/CUnittest.h"

#include "naucrates/base/CQueryToDXLResult.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/xml/CDXLMemoryManager.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

#include "unittest/dxl/CDXLUtilsTest.h"

#include <xercesc/util/Base64.hpp>

XERCES_CPP_NAMESPACE_USE

using namespace gpos;
using namespace gpdxl;

static const char *szQueryFile = "../data/dxl/expressiontests/TableScanQuery.xml";
static const char *szPlanFile = "../data/dxl/expressiontests/TableScanPlan.xml";

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtilsTest::EresUnittest
//
//	@doc:
//		Unittest for serializing XML
//
//---------------------------------------------------------------------------
GPOS_RESULT
CDXLUtilsTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CDXLUtilsTest::EresUnittest_SerializeQuery),
		GPOS_UNITTEST_FUNC(CDXLUtilsTest::EresUnittest_SerializePlan),
		GPOS_UNITTEST_FUNC(CDXLUtilsTest::EresUnittest_Encoding),
		};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtilsTest::EresUnittest_SerializeQuery
//
//	@doc:
//		Testing serialization of queries in DXL
//
//---------------------------------------------------------------------------
GPOS_RESULT
CDXLUtilsTest::EresUnittest_SerializeQuery()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();
	
	// read DXL file
	CHAR *szDXL = CDXLUtils::SzRead(pmp, szQueryFile);

	CQueryToDXLResult *presult = CDXLUtils::PdxlnParseDXLQuery(pmp, szDXL, NULL /*szXSDPath*/);
	
	// serialize with document header
	BOOL rgfIndentation[] = {true, false};
	BOOL rgfHeaders[] = {true, false};
	
	CWStringDynamic str(pmp);
	COstreamString oss(&str);

	for (ULONG ulHeaders = 0; ulHeaders < GPOS_ARRAY_SIZE(rgfHeaders); ulHeaders++)
	{
		for (ULONG ulIndent = 0; ulIndent < GPOS_ARRAY_SIZE(rgfIndentation); ulIndent++)
		{
			oss << "Headers: " << rgfHeaders[ulHeaders] << ", indentation: " << rgfIndentation[ulIndent] << std::endl;
			CDXLUtils::SerializeQuery
				(
				pmp,
				oss,
				presult->Pdxln(),
				presult->PdrgpdxlnOutputCols(),
				presult->PdrgpdxlnCTE(),
				rgfHeaders[ulHeaders],
				rgfIndentation[ulIndent]
				);
			oss << std::endl;
		}
	}
	
	
	GPOS_TRACE(str.Wsz());

	// cleanup
	GPOS_DELETE(presult);
	GPOS_DELETE_ARRAY(szDXL);
	
	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtilsTest::EresUnittest_SerializePlan
//
//	@doc:
//		Testing serialization of plans in DXL
//
//---------------------------------------------------------------------------
GPOS_RESULT
CDXLUtilsTest::EresUnittest_SerializePlan()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();
	
	// read DXL file
	CHAR *szDXL = CDXLUtils::SzRead(pmp, szPlanFile);

	ULLONG ullPlanId = gpos::ullong_max;
	ULLONG ullPlanSpaceSize = gpos::ullong_max;
	CDXLNode *pdxln = CDXLUtils::PdxlnParsePlan(pmp, szDXL, NULL /*szXSDPath*/, &ullPlanId, &ullPlanSpaceSize);
	
	// serialize with document header
	BOOL rgfIndentation[] = {true, false};
	BOOL rgfHeaders[] = {true, false};
	
	CWStringDynamic str(pmp);
	COstreamString oss(&str);

	for (ULONG ulHeaders = 0; ulHeaders < GPOS_ARRAY_SIZE(rgfHeaders); ulHeaders++)
	{
		for (ULONG ulIndent = 0; ulIndent < GPOS_ARRAY_SIZE(rgfIndentation); ulIndent++)
		{
			oss << "Headers: " << rgfHeaders[ulHeaders] << ", indentation: " << rgfIndentation[ulIndent] << std::endl;
			CDXLUtils::SerializePlan(pmp, oss, pdxln, ullPlanId, ullPlanSpaceSize, rgfHeaders[ulHeaders], rgfIndentation[ulIndent]);
			oss << std::endl;
		}
	}
	
	
	GPOS_TRACE(str.Wsz());

	// cleanup
	pdxln->Release();
	GPOS_DELETE_ARRAY(szDXL);
	
	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLUtilsTest::EresUnittest_Encoding
//
//	@doc:
//		Testing base64 encoding
//
//---------------------------------------------------------------------------
GPOS_RESULT
CDXLUtilsTest::EresUnittest_Encoding()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	CAutoP<CDXLMemoryManager> a_pmm(GPOS_NEW(pmp) CDXLMemoryManager(pmp));

	const CHAR *sz = "{\"{FUNCEXPR :funcid 1967 :funcresulttype 1184 :funcretset false :funcformat 1 :args ({FUNCEXPR :funcid 1191 :funcresulttype 1184 :funcretset false :funcformat 2 :args ({CONST :consttype 25 :constlen -1 :constbyval false :constisnull false :constvalue 7 [ 0 0 0 7 110 111 119 ]})} {CONST :consttype 23 :constlen 4 :constbyval true :constisnull false :constvalue 4 [ 2 0 0 0 0 0 0 0 ]})}\"}";
	ULONG ulLen = clib::UlStrLen(sz);
	const XMLByte *pxmlbyte = (const XMLByte *) sz;

	// encode string in base 64
	XMLSize_t outputLength = 0;
	CAutoRg<XMLByte> a_pxmlbyteEncoded;
	a_pxmlbyteEncoded = Base64::encode(pxmlbyte, (XMLSize_t) ulLen, &outputLength, a_pmm.Pt());
	CHAR *szEncoded = (CHAR *) (a_pxmlbyteEncoded.Rgt());

	// convert encoded string to array of XMLCh
	XMLCh *pxmlch = GPOS_NEW_ARRAY(pmp, XMLCh, outputLength + 1);
	for (ULONG ul = 0; ul < outputLength; ul++)
	{
		pxmlch[ul] = (XMLCh) a_pxmlbyteEncoded[ul];
	}
	pxmlch[outputLength] = 0;

	// decode encoded string
	CAutoRg<XMLByte> a_pxmlbyteDecoded;
	a_pxmlbyteDecoded = Base64::decode(a_pxmlbyteEncoded.Rgt(), &outputLength, a_pmm.Pt());
	CHAR *szDecoded = (CHAR *) (a_pxmlbyteDecoded.Rgt());
	GPOS_ASSERT(0 == clib::IStrCmp(szDecoded, sz));

	// get a byte array from XMLCh representation of encoded string
	ULONG ulOutputLen = 0;
	BYTE *pba = CDXLUtils::PbaFromBase64XMLStr(a_pmm.Pt(), pxmlch, &ulOutputLen);
	CHAR *szPba = (CHAR *) pba;
	GPOS_ASSERT(0 == clib::IStrCmp(szPba, sz));

	{
		CAutoTrace at(pmp);
		at.Os() << std::endl << "Input:" << sz << std::endl;
		at.Os() << std::endl << "Encoded:" << szEncoded << std::endl;
		at.Os() << std::endl << "Decoded:" << szDecoded << std::endl;
		at.Os() << std::endl << "Decoded from byte array:" << szPba << std::endl;
	}

	GPOS_DELETE_ARRAY(pba);
	GPOS_DELETE_ARRAY(pxmlch);

	return GPOS_OK;
}

// EOF
