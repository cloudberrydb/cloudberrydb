//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CDrvdProp.cpp
//
//	@doc:
//		Implementation of derived properties
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
    return drvdprop.OsPrint(os);
  }

}

// EOF
