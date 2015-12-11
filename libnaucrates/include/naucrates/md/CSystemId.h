//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CSystemId.h
//
//	@doc:
//		Class for representing the system id of a metadata provider
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------



#ifndef GPMD_CSystemId_H
#define GPMD_CSystemId_H

#include "gpos/base.h"

#include "naucrates/md/IMDId.h"

#define GPDXL_SYSID_LENGTH 10

namespace gpmd
{
	using namespace gpos;


	//---------------------------------------------------------------------------
	//	@class:
	//		CSystemId
	//
	//	@doc:
	//		Class for representing the system id of a metadata provider
	//
	//---------------------------------------------------------------------------
	class CSystemId
	{
		private:
		
			// system id type
			IMDId::EMDIdType m_emdidt;
					
			// system id
			WCHAR m_wsz[GPDXL_SYSID_LENGTH + 1];

		public:
			// ctor
			CSystemId(IMDId::EMDIdType emdidt, const WCHAR *wsz, ULONG ulLength = GPDXL_SYSID_LENGTH);
			
			// copy ctor
			CSystemId(const CSystemId &);
			
			// type of system id
			IMDId::EMDIdType Emdidt() const
			{
				return m_emdidt;
			}
			
			// system id string
			const WCHAR *Wsz() const
			{
				return m_wsz;
			}
			
			// equality
			BOOL FEquals(const CSystemId &sysid) const;
			
			// hash function
			ULONG UlHash() const;
	};

	// dynamic arrays over md system id elements
	typedef CDynamicPtrArray<CSystemId, CleanupDelete> DrgPsysid;
}



#endif // !GPMD_CSystemId_H

// EOF
