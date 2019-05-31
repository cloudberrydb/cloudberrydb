//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		DrvdPropArray.cpp
//
//	@doc:
//		Implementation of derived properties
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CDrvdProp.h"
#include "gpopt/operators/COperator.h"

#ifdef GPOS_DEBUG
#include "gpopt/base/COptCtxt.h"
#include "gpos/error/CAutoTrace.h"
#endif // GPOS_DEBUG

namespace gpopt {

	DrvdPropArray::DrvdPropArray()
	{}

	IOstream &operator << (IOstream &os, const DrvdPropArray &drvdprop)
	{
		return drvdprop.OsPrint(os);
	}

#ifdef GPOS_DEBUG
	void
	DrvdPropArray::DbgPrint() const
	{
		CMemoryPool *mp = COptCtxt::PoctxtFromTLS()->Pmp();
		CAutoTrace at(mp);
		at.Os() << *this;
	}
#endif // GPOS_DEBUG
}

// EOF
