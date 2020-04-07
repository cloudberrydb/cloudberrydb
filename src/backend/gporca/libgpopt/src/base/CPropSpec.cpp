//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPropSpec.cpp
//
//	@doc:
//		Abstraction for specification of properties
//---------------------------------------------------------------------------

#include "gpopt/base/CPropSpec.h"

#ifdef GPOS_DEBUG
#include "gpopt/base/COptCtxt.h"
#include "gpos/error/CAutoTrace.h"
#endif

using namespace gpopt;

#ifdef GPOS_DEBUG
// print distribution spec
void
CPropSpec::DbgPrint() const
{
	CMemoryPool *mp = COptCtxt::PoctxtFromTLS()->Pmp();
	CAutoTrace at(mp);
	at.Os() << *this;
}
#endif // GPOS_DEBUG
// EOF
