//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CStatsPredUnsupported.h
//
//	@doc:
//		Class representing unsupported statistics filter
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CStatsPredUnsupported_H
#define GPNAUCRATES_CStatsPredUnsupported_H

#include "gpos/base.h"
#include "gpos/common/CDouble.h"
#include "naucrates/statistics/CStatsPred.h"

namespace gpnaucrates
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CStatsPredUnsupported
	//
	//	@doc:
	//		Class representing unsupported statistics filter
	//---------------------------------------------------------------------------
	class CStatsPredUnsupported : public CStatsPred
	{
		private:

			// predicate comparison type
			CStatsPred::EStatsCmpType m_estatscmptype;

			// scale factor of the predicate
			CDouble m_dDefaultScaleFactor;

			// initialize the scale factor of the predicate
			CDouble DScaleFactorInit();

			// private copy ctor
			CStatsPredUnsupported(const CStatsPredUnsupported &);

		public:

			// ctors
			CStatsPredUnsupported(ULONG ulColId, CStatsPred::EStatsCmpType espt);
			CStatsPredUnsupported(ULONG ulColId, CStatsPred::EStatsCmpType espt, CDouble dDefaultScaleFactor);

			// filter type id
			virtual
			CStatsPred::EStatsPredType Espt() const
			{
				return CStatsPred::EsptUnsupported;
			}

			// comparison types for stats computation
			virtual
			CStatsPred::EStatsCmpType Estatscmptype() const
			{
				return m_estatscmptype;
			}

			CDouble DScaleFactor() const
			{
				return m_dDefaultScaleFactor;
			}

			// conversion function
			static
			CStatsPredUnsupported *PstatspredConvert
				(
				CStatsPred *pstatspred
				)
			{
				GPOS_ASSERT(NULL != pstatspred);
				GPOS_ASSERT(CStatsPred::EsptUnsupported == pstatspred->Espt());

				return dynamic_cast<CStatsPredUnsupported*>(pstatspred);
			}

	}; // class CStatsPredUnsupported
}

#endif // !GPNAUCRATES_CStatsPredUnsupported_H

// EOF
