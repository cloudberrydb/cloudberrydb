//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		IBucket.h
//
//	@doc:
//		Simple bucket interface
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPNAUCRATES_IBucket_H
#define GPNAUCRATES_IBucket_H

#include "gpos/base.h"
#include "naucrates/statistics/CPoint.h"

namespace gpnaucrates
{
	using namespace gpos;
	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		IBucket
	//
	//	@doc:
	//		Simple bucket interface. Has a lower point and upper point
	//
	//---------------------------------------------------------------------------

	class IBucket
	{
		private:

			// private copy constructor
			IBucket(const IBucket &);

			// private assignment operator
			IBucket& operator=(const IBucket &);

		public:
			// c'tor
			IBucket() {}

			// lower point
			virtual
			CPoint *PpLower() const = 0;

			// upper point
			virtual
			CPoint *PpUpper() const = 0;

			// is bucket singleton?
			BOOL FSingleton() const
			{
				return PpLower()->FEqual(PpUpper());
			}

			// d'tor
			virtual ~IBucket() {}
	};
}

#endif // !GPNAUCRATES_IBucket_H

// EOF
