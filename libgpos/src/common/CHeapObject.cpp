//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CHeapObject.cpp
//
//	@doc:
//		Implementation of class of all objects that must reside on the heap;
//---------------------------------------------------------------------------

#include "gpos/utils.h"
#include "gpos/common/CHeapObject.h"

using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		CHeapObject::CHeapObject
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CHeapObject::CHeapObject()
{
#if (GPOS_i386 || GPOS_i686 || GPOS_x86_64)
	GPOS_ASSERT(false == gpos::FOnStack(this) &&
	            "Object incorrectly allocated (stack/heap)");
#endif // (GPOS_i386 || GPOS_i686 || GPOS_x86_64)
}


// EOF

