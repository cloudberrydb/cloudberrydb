//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CDrvdProp.cpp
//
//	@doc:
//		Implementation of derived properties
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CDrvdProp.h"
#include "gpopt/operators/COperator.h"

namespace gpopt {

  //---------------------------------------------------------------------------
  //	@function:
  //		CDrvdProp::CDrvdProp
  //
  //	@doc:
  //		ctor
  //
  //---------------------------------------------------------------------------
  CDrvdProp::CDrvdProp()
  {}

  IOstream &operator << (IOstream &os, CDrvdProp &drvdprop)
  {
    // FIXME(chasseur): in well-formed C++ code, references can never be bound
    // to NULL; however, some callers may dereference a (possibly-NULL) pointer
    // with the '*' operator and try to print it into an IOStream; callers
    // should be modified to explicitly do NULL-checks on pointers so that this
    // function does not rely on undefined behavior
    return (NULL == &drvdprop) ? os : drvdprop.OsPrint(os);
  }

}

// EOF
