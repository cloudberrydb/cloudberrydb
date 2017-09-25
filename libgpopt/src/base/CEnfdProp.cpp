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
    return efdprop.OsPrint(os);
  }

}

// EOF
