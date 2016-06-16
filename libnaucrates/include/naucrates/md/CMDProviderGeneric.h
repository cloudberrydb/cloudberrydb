//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CMDProviderGeneric.h
//
//	@doc:
//		Provider of system-independent metadata objects.
//---------------------------------------------------------------------------



#ifndef GPMD_CMDProviderGeneric_H
#define GPMD_CMDProviderGeneric_H

#include "gpos/base.h"

#include "naucrates/md/IMDId.h"
#include "naucrates/md/IMDType.h"

#define GPMD_DEFAULT_SYSID GPOS_WSZ_LIT("GPDB")

namespace gpmd
{
	using namespace gpos;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CMDProviderGeneric
	//
	//	@doc:
	//		Provider of system-independent metadata objects.
	//
	//---------------------------------------------------------------------------
	class CMDProviderGeneric
	{

		private:
			// mdid of int2
			IMDId *m_pmdidInt2;

			// mdid of int4
			IMDId *m_pmdidInt4;
			
			// mdid of int8
			IMDId *m_pmdidInt8;

			// mdid of bool
			IMDId *m_pmdidBool;

			// mdid of oid
			IMDId *m_pmdidOid;

			// private copy ctor
			CMDProviderGeneric(const CMDProviderGeneric&);
			
		public:
			// ctor/dtor
			CMDProviderGeneric(IMemoryPool *pmp);
			
			// dtor
			~CMDProviderGeneric();
			
			// return the mdid for the requested type
			IMDId *Pmdid(IMDType::ETypeInfo eti) const;
			
			// default system id
			CSystemId SysidDefault() const;
	};
}



#endif // !GPMD_CMDProviderGeneric_H

// EOF
