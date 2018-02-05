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
			CAutoMemoryPool m_amp;
			CMDAccessor m_mda;
			CAutoOptCtxt m_aoc;
			CScalarIsDistinctFrom* m_pScalarIDF;

		public:
			SEberFixture():
					m_mda(m_amp.Pmp(), CMDCache::Pcache(), CTestUtils::m_sysidDefault, CTestUtils::m_pmdpf),
					m_aoc(m_amp.Pmp(), &m_mda, NULL /* pceeval */, CTestUtils::Pcm(m_amp.Pmp()))
			{
				CMDIdGPDB *pmdidEqOp = GPOS_NEW(m_amp.Pmp()) CMDIdGPDB(GPDB_INT4_EQ_OP);
				const CWStringConst *pwsOpName = GPOS_NEW(Pmp()) CWStringConst(GPOS_WSZ_LIT("="));

				m_pScalarIDF = GPOS_NEW(m_amp.Pmp()) CScalarIsDistinctFrom
														(Pmp(),
														 pmdidEqOp,
														 pwsOpName
														);
				CTestUtils::m_pmdpf->AddRef();
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
