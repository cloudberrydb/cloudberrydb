//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 Pivotal Software, Inc.
//
//	@filename:
//		CEnfdProp.cpp
//
//	@doc:
//		Implementation of enforced property
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CEnfdProp.h"

namespace gpopt {

  IOstream &operator << (IOstream &os, CEnfdProp &efdprop)
  {
    // FIXME(chasseur): in well-formed C++ code, references can never be bound
    // to NULL; however, some callers may dereference a (possibly-NULL) pointer
    // with the '*' operator and try to print it into an IOStream; callers
    // should be modified to explicitly do NULL-checks on pointers so that this
    // function does not rely on undefined behavior
    return (NULL == &efdprop) ? os : efdprop.OsPrint(os);
  }

}

// EOF
