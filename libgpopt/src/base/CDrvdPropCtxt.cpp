//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 Pivotal Software, Inc.
//
//	@filename:
//		CDrvdPropCtxt.cpp
//
//	@doc:
//		Implementation of derived properties context
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CDrvdPropCtxt.h"

namespace gpopt {

  IOstream &operator << (IOstream &os, CDrvdPropCtxt &drvdpropctxt)
  {
    return drvdpropctxt.OsPrint(os);
  }

}

// EOF
