//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CMDProviderMemory.h
//
//	@doc:
//		Memory-based provider of metadata objects.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------



#ifndef GPMD_CMDProviderMemory_H
#define GPMD_CMDProviderMemory_H

#include "gpos/base.h"
#include "gpos/string/CWStringDynamic.h"

#include "naucrates/md/IMDId.h"
#include "naucrates/md/IMDCacheObject.h"
#include "naucrates/md/IMDProvider.h"

namespace gpmd
{
	using namespace gpos;
	using namespace gpnaucrates;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CMDProviderMemory
	//
	//	@doc:
	//		Memory-based provider of metadata objects.
	//
	//---------------------------------------------------------------------------
	class CMDProviderMemory : public IMDProvider
	{
		protected:
	
			// hash map of serialized MD objects indexed by their MD id 
			typedef CHashMap<IMDId, CWStringDynamic, 
							IMDId::UlHashMDId, IMDId::FEqualMDId,
							CleanupRelease, CleanupDelete> MDMap;
			
			// metadata objects indexed by their metadata id
			MDMap *m_pmdmap;
			
			// load MD objects in the hash map
			void LoadMetadataObjectsFromArray(IMemoryPool *pmp, DrgPimdobj *pdrgpmdobj);
			
			// private copy ctor
			CMDProviderMemory(const CMDProviderMemory&);
				
		public:
			
			// ctor
			CMDProviderMemory(IMemoryPool *pmp, DrgPimdobj *pdrgpmdobj);
			
			// ctor
			CMDProviderMemory(IMemoryPool *pmp, const CHAR *szFileName);
			
			//dtor
			virtual 
			~CMDProviderMemory();

			// returns the DXL string of the requested metadata object
			virtual 
			CWStringBase *PstrObject(IMemoryPool *pmp, CMDAccessor *pmda, IMDId *pmdid) const;
			
			// return the mdid for the specified system id and type
			virtual
			IMDId *Pmdid(IMemoryPool *pmp, CSystemId sysid, IMDType::ETypeInfo eti) const;
	};
}



#endif // !GPMD_CMDProviderMemory_H

// EOF
