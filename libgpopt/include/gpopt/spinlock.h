//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		spinlock.h
//
//	@doc:
//		Definition of GPOPT-specific spinlock types
//---------------------------------------------------------------------------
#ifndef GPOPT_spinlock_H
#define GPOPT_spinlock_H

#include "gpos/sync/CSpinlock.h"

namespace gpopt
{
	using namespace gpos;

	// OPTIMIZER SPINLOCKS - reserve range 200-400

	// spinlock used in job queues
	typedef CSpinlockRanked<210> CSpinlockJobQueue;

	// spinlock used in column factory
	typedef CSpinlockRanked<220> CSpinlockColumnFactory;

	// spinlocks to synchronize memo access
	typedef CSpinlockRanked<230> CSpinlockGroup;
	typedef CSpinlockRanked<231> CSpinlockMemo;

	// spinlock used in metadata accessor cache accessor hashtable
	typedef CSpinlockRanked<240> CSpinlockMDAcc;

	// spinlock used in metadata accessor provider hashtable
	typedef CSpinlockRanked<241> CSpinlockMDAccMDP;

	// spinlock used in hashtable for optimization contexts
	typedef CSpinlockRanked<250> CSpinlockOC;

	// spinlock used in hashtable for cost contexts
	typedef CSpinlockRanked<260> CSpinlockCC;
}

#endif // !GPOPT_spinlock_H


// EOF

