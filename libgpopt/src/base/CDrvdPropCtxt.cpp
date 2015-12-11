//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 Pivotal Software, Inc.
//
//	@filename:
//		CDrvdPropCtxt.cpp
//
//	@doc:
//		Implementation of derived properties context
//
//	@owner:
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CDrvdPropCtxt.h"

namespace gpopt {

  IOstream &operator << (IOstream &os, CDrvdPropCtxt &drvdpropctxt)
  {
    // FIXME(chasseur): in well-formed C++ code, references can never be bound
    // to NULL; however, some callers may dereference a (possibly-NULL) pointer
    // with the '*' operator and try to print it into an IOStream; callers
    // should be modified to explicitly do NULL-checks on pointers so that this
    // function does not rely on undefined behavior
    return (NULL == &drvdpropctxt) ? os : drvdpropctxt.OsPrint(os);
  }

}

// EOF
