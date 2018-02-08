#include "unittest/gpopt/operators/CScalarIsDistinctFromTest.h"

#include "gpos/common/CDynamicPtrArray.h"

#include "gpopt/operators/CScalarIsDistinctFrom.h"
#include "gpos/string/CWStringConst.h"
#include "unittest/gpopt/CTestUtils.h"

namespace gpopt
{
	using namespace gpos;

	class SEberFixture
	{
		private:
			const CAutoMemoryPool m_amp;
			CMDAccessor m_mda;
			const CAutoOptCtxt m_aoc;
			CScalarIsDistinctFrom* const m_pScalarIDF;

			static IMDProvider* Pmdp()
			{
				CTestUtils::m_pmdpf->AddRef();
				return CTestUtils::m_pmdpf;
			}
		public:
			SEberFixture():
					m_amp(),
					m_mda(m_amp.Pmp(), CMDCache::Pcache(), CTestUtils::m_sysidDefault, Pmdp()),
					m_aoc(m_amp.Pmp(), &m_mda, NULL /* pceeval */, CTestUtils::Pcm(m_amp.Pmp())),
					m_pScalarIDF(GPOS_NEW(m_amp.Pmp()) CScalarIsDistinctFrom(
														Pmp(),
														GPOS_NEW(m_amp.Pmp()) CMDIdGPDB(GPDB_INT4_EQ_OP),
														GPOS_NEW(m_amp.Pmp()) CWStringConst(GPOS_WSZ_LIT("="))
														))
			{
			}

			~SEberFixture()
			{
				m_pScalarIDF->Release();
			}

			IMemoryPool* Pmp() const
			{
				return m_amp.Pmp();
			}

			CScalarIsDistinctFrom *PScalarIDF() const
			{
				return m_pScalarIDF;
			}
	};

	static GPOS_RESULT EresUnittest_Eber_WhenBothInputsAreNull()
	{
		SEberFixture fixture;
		IMemoryPool *pmp = fixture.Pmp();

		DrgPul *pdrgpulChildren = GPOS_NEW(pmp) DrgPul(pmp);
		pdrgpulChildren->Append(GPOS_NEW(pmp) ULONG(CScalar::EberNull));
		pdrgpulChildren->Append(GPOS_NEW(pmp) ULONG(CScalar::EberNull));

		CScalarIsDistinctFrom *pScalarIDF = fixture.PScalarIDF();

		CScalar::EBoolEvalResult eberResult = pScalarIDF->Eber(pdrgpulChildren);
		GPOS_RTL_ASSERT(eberResult == CScalar::EberFalse);

		pdrgpulChildren->Release();

		return GPOS_OK;
	}

	static GPOS_RESULT EresUnittest_Eber_WhenFirstInputIsUnknown()
	{
		SEberFixture fixture;
		IMemoryPool *pmp = fixture.Pmp();

		DrgPul *pdrgpulChildren = GPOS_NEW(pmp) DrgPul(pmp);
		pdrgpulChildren->Append(GPOS_NEW(pmp) ULONG(CScalar::EberUnknown));
		pdrgpulChildren->Append(GPOS_NEW(pmp) ULONG(CScalar::EberNull));

		CScalarIsDistinctFrom *pScalarIDF = fixture.PScalarIDF();

		CScalar::EBoolEvalResult eberResult = pScalarIDF->Eber(pdrgpulChildren);
		GPOS_RTL_ASSERT(eberResult == CScalar::EberUnknown);

		pdrgpulChildren->Release();

		return GPOS_OK;
	}

	static GPOS_RESULT EresUnittest_Eber_WhenSecondInputIsUnknown()
	{
		SEberFixture fixture;
		IMemoryPool *pmp = fixture.Pmp();

		DrgPul *pdrgpulChildren = GPOS_NEW(pmp) DrgPul(pmp);
		pdrgpulChildren->Append(GPOS_NEW(pmp) ULONG(CScalar::EberNull));
		pdrgpulChildren->Append(GPOS_NEW(pmp) ULONG(CScalar::EberUnknown));

		CScalarIsDistinctFrom *pScalarIDF = fixture.PScalarIDF();

		CScalar::EBoolEvalResult eberResult = pScalarIDF->Eber(pdrgpulChildren);
		GPOS_RTL_ASSERT(eberResult == CScalar::EberUnknown);

		pdrgpulChildren->Release();

		return GPOS_OK;
	}

	static GPOS_RESULT EresUnittest_Eber_WhenFirstInputDiffersFromSecondInput()
	{
		SEberFixture fixture;
		IMemoryPool *pmp = fixture.Pmp();

		DrgPul *pdrgpulChildren = GPOS_NEW(pmp) DrgPul(pmp);
		pdrgpulChildren->Append(GPOS_NEW(pmp) ULONG(CScalar::EberNull));
		pdrgpulChildren->Append(GPOS_NEW(pmp) ULONG(CScalar::EberTrue));

		CScalarIsDistinctFrom *pScalarIDF = fixture.PScalarIDF();

		CScalar::EBoolEvalResult eberResult = pScalarIDF->Eber(pdrgpulChildren);
		GPOS_RTL_ASSERT(eberResult == CScalar::EberTrue);

		pdrgpulChildren->Release();

		return GPOS_OK;
	}

	static GPOS_RESULT EresUnittest_Eber_WhenBothInputsAreSameAndNotNull()
	{
		SEberFixture fixture;
		IMemoryPool *pmp = fixture.Pmp();

		DrgPul *pdrgpulChildren = GPOS_NEW(pmp) DrgPul(pmp);
		pdrgpulChildren->Append(GPOS_NEW(pmp) ULONG(CScalar::EberTrue));
		pdrgpulChildren->Append(GPOS_NEW(pmp) ULONG(CScalar::EberTrue));

		CScalarIsDistinctFrom *pScalarIDF = fixture.PScalarIDF();

		CScalar::EBoolEvalResult eberResult = pScalarIDF->Eber(pdrgpulChildren);
		GPOS_RTL_ASSERT(eberResult == CScalar::EberFalse);

		pdrgpulChildren->Release();

		return GPOS_OK;
	}

	GPOS_RESULT CScalarIsDistinctFromTest::EresUnittest()
	{
		CUnittest rgut[] =
				{
				GPOS_UNITTEST_FUNC(EresUnittest_Eber_WhenBothInputsAreNull),
				GPOS_UNITTEST_FUNC(EresUnittest_Eber_WhenFirstInputIsUnknown),
				GPOS_UNITTEST_FUNC(EresUnittest_Eber_WhenSecondInputIsUnknown),
				GPOS_UNITTEST_FUNC(EresUnittest_Eber_WhenFirstInputDiffersFromSecondInput),
				GPOS_UNITTEST_FUNC(EresUnittest_Eber_WhenBothInputsAreSameAndNotNull),
				};

		return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
	}
}
