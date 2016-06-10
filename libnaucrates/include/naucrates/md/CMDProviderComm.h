//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CMDProviderComm.h
//
//	@doc:
//		Communicator-based provider of metadata objects.
//---------------------------------------------------------------------------



#ifndef GPMD_CMDProviderComm_H
#define GPMD_CMDProviderComm_H

#include "gpos/base.h"
#include "gpos/string/CWStringDynamic.h"

#include "naucrates/md/IMDId.h"
#include "naucrates/md/IMDCacheObject.h"
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
	//		CMDProviderComm
	//
	//	@doc:
	//		Communicator-based provider of metadata objects.
	//
	//---------------------------------------------------------------------------
	class CMDProviderComm : public IMDProvider
	{
		
		private:
		
			// memory pool
			IMemoryPool *m_pmp;
			
			// communicator
			CCommunicator *m_pcomm;
						
			// send a request for metadata and return the response
			CWStringBase *PstrRequestMD(IMemoryPool *pmp, const CWStringDynamic *pstrRequest) const;
			
			// private copy ctor
			CMDProviderComm(const CMDProviderComm &);
			
		public:
			// ctor
			CMDProviderComm(IMemoryPool *pmp, CCommunicator *pcomm);
			
			// dtor
			virtual 
			~CMDProviderComm();

			// returns the DXL string of the requested metadata object
			virtual 
			CWStringBase *PstrObject(IMemoryPool *pmp, CMDAccessor *pmda, IMDId *pmdid) const;
			
			// return the mdid for the specified system id and type
			virtual
			IMDId *Pmdid(IMemoryPool *pmp, CSystemId sysid, IMDType::ETypeInfo eti) const;

	};
}



#endif // !GPMD_CMDProviderComm_H

// EOF
