//	Greenplum Database
//	Copyright (C) 2016 Pivotal Software, Inc.
#ifndef GPOPT_CPhysicalUnionAllFactory_H
#define GPOPT_CPhysicalUnionAllFactory_H

#include "gpos/types.h"
#include "gpopt/operators/CLogicalUnionAll.h"
#include "gpopt/operators/CPhysicalUnionAll.h"

namespace gpopt
{
	// Constructs a gpopt::CPhysicalUnionAll operator instance. Depending the
	// parameter fParallel we construct either a CPhysicalParallelUnionAll or
	// a CPhysicalSerialUnionAll instance.
	class CPhysicalUnionAllFactory
	{
		private:
			CLogicalUnionAll* const m_popLogicalUnionAll;
			const BOOL m_fParallel;

		public:

			CPhysicalUnionAllFactory(CLogicalUnionAll* popLogicalUnionAll, BOOL fParallel);

			CPhysicalUnionAll* PopPhysicalUnionAll(IMemoryPool* pmp);
	};

}

#endif //GPOPT_CPhysicalUnionAllFactory_H
