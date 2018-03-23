//	Greenplum Database
//	Copyright (C) 2018 Pivotal, Inc.



#include <xercesc/util/XercesDefs.hpp>
#include <xercesc/sax2/SAX2XMLReader.hpp>
#include <xercesc/sax2/XMLReaderFactory.hpp>
#include <xercesc/framework/MemBufInputSource.hpp>

#include "unittest/dxl/CParseHandlerCostModelTest.h"

#include <memory>

#include "gpdbcost/CCostModelGPDB.h"
#include "gpdbcost/CCostModelParamsGPDB.h"
#include "gpdbcost/CCostModelParamsGPDBLegacy.h"


#include "gpos/base.h"
#include "gpos/test/CUnittest.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/common/CAutoP.h"
#include "gpos/common/CAutoRef.h"
#include "gpos/common/CAutoRg.h"
#include "gpos/io/COstreamString.h"
#include "naucrates/dxl/parser/CParseHandlerCostModel.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"
#include "naucrates/dxl/xml/CDXLMemoryManager.h"
#include "naucrates/dxl/CCostModelConfigSerializer.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpdxl;
using gpdbcost::CCostModelGPDB;
using gpdbcost::CCostModelParamsGPDB;

XERCES_CPP_NAMESPACE_USE


namespace
{
	class Fixture
	{
		private:
			CAutoMemoryPool m_amp;
			gpos::CAutoP<CDXLMemoryManager> m_apmm;
			std::auto_ptr<SAX2XMLReader> m_apxmlreader;
			gpos::CAutoP<CParseHandlerManager> m_apphm;
			gpos::CAutoP<CParseHandlerCostModel> m_apphCostModel;

	public:
			Fixture():
					m_apmm(GPOS_NEW(Pmp()) CDXLMemoryManager(Pmp())),
					m_apxmlreader(XMLReaderFactory::createXMLReader(Pmm())),
					m_apphm(GPOS_NEW(Pmp()) CParseHandlerManager(Pmm(), Pxmlreader())),
					m_apphCostModel(GPOS_NEW(Pmp()) CParseHandlerCostModel(Pmp(), Pphm(), NULL))
			{
				m_apphm->ActivateParseHandler(PphCostModel());
			}

			IMemoryPool *Pmp() const
			{
				return m_amp.Pmp();
			}

			CDXLMemoryManager *Pmm()
			{
				return m_apmm.Pt();
			}

			SAX2XMLReader *Pxmlreader()
			{
				return m_apxmlreader.get();
			}

			CParseHandlerManager *Pphm()
			{
				return m_apphm.Pt();
			}

			CParseHandlerCostModel *PphCostModel()
			{
				return m_apphCostModel.Pt();
			}

			void Parse(const XMLByte szDXL[], size_t size)
			{
				MemBufInputSource mbis(
					szDXL,
					size,
					"dxl test",
					false,
					Pmm()
				);
				Pxmlreader()->parse(mbis);
			}
	};
}

static gpos::GPOS_RESULT Eres_ParseCalibratedCostModel()
{
	const CHAR szDXLFileName[] = "../data/dxl/parse_tests/CostModelConfigCalibrated.xml";
	Fixture fixture;

	IMemoryPool *pmp = fixture.Pmp();

	gpos::CAutoRg<CHAR> a_szDXL(CDXLUtils::SzRead(pmp, szDXLFileName));

	CParseHandlerCostModel *pphcm = fixture.PphCostModel();

	fixture.Parse((const XMLByte *)a_szDXL.Rgt(), strlen(a_szDXL.Rgt()));

	ICostModel *pcm = pphcm->Pcm();

	GPOS_RTL_ASSERT(ICostModel::EcmtGPDBCalibrated == pcm->Ecmt());
	GPOS_RTL_ASSERT(3 == pcm->UlHosts());

	CAutoRef<CCostModelParamsGPDB> pcpExpected(GPOS_NEW(pmp) CCostModelParamsGPDB(pmp));
	pcpExpected->SetParam(CCostModelParamsGPDB::EcpNLJFactor, 1024.0, 1023.0, 1025.0);
	GPOS_RTL_ASSERT(pcpExpected->FEquals(pcm->Pcp()));


	return gpos::GPOS_OK;
}

static gpos::GPOS_RESULT Eres_SerializeCalibratedCostModel()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	const WCHAR *const wszExpectedString = L"<dxl:CostModelConfig CostModelType=\"1\" SegmentsForCosting=\"3\">"
								   "<dxl:CostParams>"
								   "<dxl:CostParam Name=\"NLJFactor\" Value=\"1024.000000\" LowerBound=\"1023.000000\" UpperBound=\"1025.000000\"/>"
								   "</dxl:CostParams>"
								   "</dxl:CostModelConfig>";
	gpos::CAutoP<CWStringDynamic> apwsExpected(GPOS_NEW(pmp) CWStringDynamic(pmp, wszExpectedString));

	const ULONG ulSegments = 3;
	CCostModelParamsGPDB *pcp = GPOS_NEW(pmp) CCostModelParamsGPDB(pmp);
	pcp->SetParam(CCostModelParamsGPDB::EcpNLJFactor, 1024.0, 1023.0, 1025.0);
	gpos::CAutoRef<CCostModelGPDB> apcm(GPOS_NEW(pmp) CCostModelGPDB(pmp, ulSegments, pcp));

	CWStringDynamic wsActual(pmp);
	COstreamString os(&wsActual);
	CXMLSerializer xmlser(pmp, os, false);
	CCostModelConfigSerializer cmcSerializer(apcm.Pt());
	cmcSerializer.Serialize(xmlser);

	GPOS_RTL_ASSERT(apwsExpected->FEquals(&wsActual));

	return gpos::GPOS_OK;
}

static gpos::GPOS_RESULT Eres_ParseLegacyCostModel()
{
	const CHAR szDXLFileName[] = "../data/dxl/parse_tests/CostModelConfigLegacy.xml";
	Fixture fixture;

	IMemoryPool *pmp = fixture.Pmp();

	gpos::CAutoRg<CHAR> a_szDXL(CDXLUtils::SzRead(pmp, szDXLFileName));

	CParseHandlerCostModel *pphcm = fixture.PphCostModel();

	fixture.Parse((const XMLByte *)a_szDXL.Rgt(), strlen(a_szDXL.Rgt()));

	ICostModel *pcm = pphcm->Pcm();

	GPOS_RTL_ASSERT(ICostModel::EcmtGPDBLegacy == pcm->Ecmt());
	GPOS_RTL_ASSERT(3 == pcm->UlHosts());

	CAutoRef<CCostModelParamsGPDBLegacy> pcpExpected(GPOS_NEW(pmp) CCostModelParamsGPDBLegacy(pmp));
	GPOS_RTL_ASSERT(pcpExpected->FEquals(pcm->Pcp()));

	return gpos::GPOS_OK;
}

gpos::GPOS_RESULT CParseHandlerCostModelTest::EresUnittest()
{
	CUnittest rgut[] =
			{
				GPOS_UNITTEST_FUNC(Eres_ParseCalibratedCostModel),
				GPOS_UNITTEST_FUNC(Eres_SerializeCalibratedCostModel),
				GPOS_UNITTEST_FUNC(Eres_ParseLegacyCostModel),
			};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}
