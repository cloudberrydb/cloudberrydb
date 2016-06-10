//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CStatsPredPoint.h
//
//	@doc:
//		Point filter on statistics
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CStatsPredPoint_H
#define GPNAUCRATES_CStatsPredPoint_H

#include "gpos/base.h"
#include "naucrates/md/IMDType.h"
#include "naucrates/statistics/CPoint.h"
#include "naucrates/statistics/CStatsPred.h"

// fwd declarations
namespace gpopt
{
	class CColRef;
}

namespace gpnaucrates
{
	using namespace gpos;
	using namespace gpmd;
	using namespace gpopt;

	//---------------------------------------------------------------------------
	//	@class:
	//		CStatsPredPoint
	//
	//	@doc:
	//		Point filter on statistics
	//---------------------------------------------------------------------------
	class CStatsPredPoint : public CStatsPred
	{
		private:

			// private copy ctor
			CStatsPredPoint(const CStatsPredPoint &);

			// private assignment operator
			CStatsPredPoint& operator=(CStatsPredPoint &);

			// comparison type
			CStatsPred::EStatsCmpType m_escmpt;

			// point to be used for comparison
			CPoint *m_ppoint;

			// add padding to datums when needed
			static
			IDatum *PdatumPreprocess(IMemoryPool *pmp, const CColRef *pcr, IDatum *pdatum);

		public:

			// ctor
			CStatsPredPoint
				(
				ULONG ulColId,
				CStatsPred::EStatsCmpType escmpt,
				CPoint *ppoint
				);

			// ctor
			CStatsPredPoint
				(
				IMemoryPool *pmp,
				const CColRef *pcr,
				CStatsPred::EStatsCmpType escmpt,
				IDatum *pdatum
				);

			// dtor
			virtual
			~CStatsPredPoint()
			{
				m_ppoint->Release();
			}

			// comparison types for stats computation
			virtual
			CStatsPred::EStatsCmpType Escmpt() const
			{
				return m_escmpt;
			}

			// filter point
			virtual
			CPoint *Ppoint() const
			{
				return m_ppoint;
			}

			// filter type id
			virtual
			EStatsPredType Espt() const
			{
				return CStatsPred::EsptPoint;
			}

			// conversion function
			static
			CStatsPredPoint *PstatspredConvert
				(
				CStatsPred *pstatspred
				)
			{
				GPOS_ASSERT(NULL != pstatspred);
				GPOS_ASSERT(CStatsPred::EsptPoint == pstatspred->Espt());

				return dynamic_cast<CStatsPredPoint*>(pstatspred);
			}

	}; // class CStatsPredPoint

}

#endif // !GPNAUCRATES_CStatsPredPoint_H

// EOF
