//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CCTEConfig.h
//
//	@doc:
//		CTE configurations
//---------------------------------------------------------------------------
#ifndef GPOPT_CCTEConfig_H
#define GPOPT_CCTEConfig_H

#include "gpos/base.h"
#include "gpos/memory/IMemoryPool.h"
#include "gpos/common/CRefCount.h"
#include "gpos/common/CDouble.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CCTEConfig
	//
	//	@doc:
	//		CTE configurations
	//
	//---------------------------------------------------------------------------
	class CCTEConfig : public CRefCount
	{

		private:

			// CTE inlining cut-off
			ULONG m_ulCTEInliningCutoff;

			// private copy ctor
			CCTEConfig(const CCTEConfig &);

		public:

			// ctor
			CCTEConfig
				(
				ULONG ulCTEInliningCutoff
				)
				:
				m_ulCTEInliningCutoff(ulCTEInliningCutoff)
			{}

			// CTE inlining cut-off
			ULONG UlCTEInliningCutoff() const
			{
				return m_ulCTEInliningCutoff;
			}

			// generate default optimizer configurations
			static
			CCTEConfig *PcteconfDefault(IMemoryPool *pmp)
			{
				return GPOS_NEW(pmp) CCTEConfig(0 /* ulCTEInliningCutoff */);
			}

	}; // class CCTEConfig
}

#endif // !GPOPT_CCTEConfig_H

// EOF
