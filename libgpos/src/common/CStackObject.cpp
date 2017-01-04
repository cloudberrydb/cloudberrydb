//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CStackObject.cpp
//
//	@doc:
//		Implementation of classes of all objects that must reside on the stack;
//---------------------------------------------------------------------------

#include "gpos/utils.h"
#include "gpos/common/CStackObject.h"

using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		CStackObject::CStackObject
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CStackObject::CStackObject()
{
#if (GPOS_i386 || GPOS_i686 || GPOS_x86_64)
	GPOS_ASSERT(true == gpos::FOnStack(this) &&
	            "Object incorrectly allocated (stack/heap)");
#endif // (GPOS_i386 || GPOS_i686 || GPOS_x86_64)
}


// EOF

