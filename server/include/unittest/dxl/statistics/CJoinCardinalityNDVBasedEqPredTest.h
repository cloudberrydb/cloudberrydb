//	Greenplum Database
//	Copyright (C) 2018 Pivotal, Inc.

#ifndef GPNAUCRATES_CJoinCardinalityNDVBasedEqPredTest_H
#define GPNAUCRATES_CJoinCardinalityNDVBasedEqPredTest_H

#include "gpos/base.h"

namespace gpnaucrates
{
	class CJoinCardinalityNDVBasedEqPredTest
	{
		private:
			static gpos::GPOS_RESULT EresUnittest_NDVEqCardEstimation();
			static gpos::GPOS_RESULT EresUnittest_NDVCardEstimationNotApplicableMultipleIdents();
			static gpos::GPOS_RESULT EresUnittest_NDVCardEstimationNotApplicableInequalityRange();

		public:
			static gpos::GPOS_RESULT EresUnittest();
	};
}

#endif // !GPNAUCRATES_CJoinCardinalityNDVBasedEqPredTest_H


// EOF
