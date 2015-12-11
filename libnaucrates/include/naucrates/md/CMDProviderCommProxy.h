//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CMDProviderCommProxy.h
//
//	@doc:
//		Proxy for answering communicator-based MD requests
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPMD_CMDProviderCommProxy_H
#define GPMD_CMDProviderCommProxy_H

#include "gpos/base.h"
#include "gpos/string/CWStringDynamic.h"

#include "naucrates/md/IMDId.h"
#include "naucrates/md/IMDProvider.h"

// fwd decl
namespace gpnaucrates
{
	class CCommunicator;
}

namespace gpopt
{
	class CMDAccessor;
}

namespace gpmd
{
	using namespace gpos;
	using namespace gpnaucrates;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CMDProviderCommProxy
	//
	//	@doc:
	//		Proxy for answering communicator-based requests for metadata
	//
	//---------------------------------------------------------------------------
	class CMDProviderCommProxy
	{
		private:
			// memory pool
			IMemoryPool *m_pmp;
			
			// underlying MD provider
			IMDProvider *m_pmdp;
			
			// private copy ctor
			CMDProviderCommProxy(const CMDProviderCommProxy&);			
			
		public:
			// ctor
			CMDProviderCommProxy(IMemoryPool *pmp, IMDProvider *pmdp);
			
			// dtor
			~CMDProviderCommProxy();

			// returns the DXL string of the requested metadata objects
			CWStringBase *PstrObject(const WCHAR *wszMDRequest) const;

	};
}

#endif // !GPMD_CMDProviderCommProxy_H

// EOF
