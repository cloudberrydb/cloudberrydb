//	Greenplum Database
//	Copyright (C) 2016 Pivotal Software, Inc.

#include "gpopt/mdcache/CMDCache.h"
#include "gpopt/minidump/CMetadataAccessorFactory.h"
#include "gpos/common/CAutoRef.h"
#include "naucrates/md/CMDProviderMemory.h"

namespace gpopt
{
	CMetadataAccessorFactory::CMetadataAccessorFactory
		(
			IMemoryPool *pmp,
			CDXLMinidump *pdxlmd,
			const CHAR *szFileName
		)
	{

		// set up MD providers
		CAutoRef<CMDProviderMemory> apmdp(GPOS_NEW(pmp) CMDProviderMemory(pmp, szFileName));
		const DrgPsysid *pdrgpsysid = pdxlmd->Pdrgpsysid();
		CAutoRef<DrgPmdp> apdrgpmdp(GPOS_NEW(pmp) DrgPmdp(pmp));

		// ensure there is at least ONE system id
		apmdp->AddRef();
		apdrgpmdp->Append(apmdp.Pt());

		for (ULONG ul = 1; ul < pdrgpsysid->UlLength(); ul++)
		{
			apmdp->AddRef();
			apdrgpmdp->Append(apmdp.Pt());
		}

		m_apmda = GPOS_NEW(pmp) CMDAccessor(pmp, CMDCache::Pcache(), pdxlmd->Pdrgpsysid(), apdrgpmdp.Pt());
	}

	CMDAccessor *CMetadataAccessorFactory::Pmda()
	{
		return m_apmda.Pt();
	}
}
