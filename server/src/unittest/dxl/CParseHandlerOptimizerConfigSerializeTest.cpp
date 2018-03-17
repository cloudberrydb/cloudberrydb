//	Greenplum Database
//	Copyright (C) 2018 Pivotal, Inc.

#include "unittest/dxl/CParseHandlerOptimizerConfigSerializeTest.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"
 #include "unittest/gpopt/CTestUtils.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "naucrates/dxl/CDXLUtils.h"

namespace
{
	class Fixture
	{
		private:
			const CAutoMemoryPool m_amp;
			CAutoRg<CHAR> m_szDXL;
		public:
			Fixture():
					m_amp(),
					m_szDXL(NULL)
			{
			}

			IMemoryPool *Pmp() const
			{
				return m_amp.Pmp();
			}

			const CHAR *SzValidationPath(BOOL fValidate)
			{
				if (fValidate)
				{
					return CTestUtils::m_szXSDPath;
				}
				else
				{
					return NULL;
				}
			}

			const CHAR *SzDxl(const CHAR *szDXLFileName)
			{
				m_szDXL = CDXLUtils::SzRead(Pmp(), szDXLFileName);

				GPOS_CHECK_ABORT;

				return m_szDXL.Rgt();
			}
	};
}

static void
SerializeOptimizerConfig
		(
		IMemoryPool *pmp,
		COptimizerConfig *poconf,
		COstream &oos,
		BOOL fIndent
		)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != poconf);

	CXMLSerializer xmlser(pmp, oos, fIndent);

	// Add XML version and encoding, DXL document header, and namespace
	CDXLUtils::SerializeHeader(pmp, &xmlser);

	// Make a dummy bitset
	CBitSet *pbs = GPOS_NEW(pmp) CBitSet(pmp, 256);

	poconf->Serialize(pmp, &xmlser, pbs);

	// Add DXL document footer
	CDXLUtils::SerializeFooter(&xmlser);

	pbs->Release();
	return;
}

namespace gpdxl
{
	// Optimizer Config request file
	const CHAR *szDXLFileName = "../data/dxl/parse_tests/OptimizerConfig.xml";

	// Parse an optimizer config and verify correctness of serialization.
	// Serialization of COptimizerConfig is only done for writing to a DXL file as part of creating a minidump
	GPOS_RESULT CParseHandlerOptimizerConfigSerializeTest::EresUnittest()
	{
		BOOL fValidate = false;
		Fixture f;
		IMemoryPool *pmp = f.Pmp();
		// Valid input for this test requires DXL in the form of:
		// Please note that most editors will automatically add a newline at the end of the file
		// This will cause the test to fail, as we do a byte-wise string comparison as opposed to a
		// comparison of canonicalized XML
		const CHAR *szDXL = f.SzDxl(szDXLFileName);
		const CHAR *szValidationPath = f.SzValidationPath(fValidate);

		CWStringDynamic str(pmp);
		COstreamString oss(&str);

		COptimizerConfig *poc = CDXLUtils::PoptimizerConfigParseDXL(pmp, szDXL, szValidationPath);

		GPOS_ASSERT(NULL != poc);

		GPOS_CHECK_ABORT;

		// Though the serialization of an optimizer config will include a traceflags element
		// This test tests only the serializing of the traceflags element itself and not the traceflag values
		// The production code calls a method to get the traceflags from a global task context
		SerializeOptimizerConfig(pmp, poc, oss, false);
		GPOS_CHECK_ABORT;

		CWStringDynamic strExpected(pmp);
		strExpected.AppendFormat(GPOS_WSZ_LIT("%s"), szDXL);

		GPOS_ASSERT(strExpected.FEquals(&str));

		poc->Release();

		return GPOS_OK;
	}
}

// EOF
