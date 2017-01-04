//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		ITimer.h
//
//	@doc:
//		A timer which records time between construction and the UlElapsedMS call;
//---------------------------------------------------------------------------
#ifndef GPOS_ITimer_H
#define GPOS_ITimer_H

#include "gpos/types.h"
#include "gpos/utils.h"

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		ITimer
	//
	//	@doc:
    //		Timer interface;
	//
	//---------------------------------------------------------------------------
	class ITimer
	{
		private:
		
			// private copy ctor
			ITimer(const ITimer&);
	
		public:

			// ctor
			ITimer()
			{}

			// dtor
			virtual
			~ITimer()
			{}

			// retrieve elapsed time in micro-seconds
			virtual
			ULONG UlElapsedUS() const = 0;

			// retrieve elapsed time in milli-seconds
			ULONG UlElapsedMS() const
			{
				return UlElapsedUS() / GPOS_USEC_IN_MSEC;
			}

			// restart timer
			virtual
			void Restart() = 0;

	}; // class ITimer

}

#endif // !GPOS_ITimer_H

// EOF

