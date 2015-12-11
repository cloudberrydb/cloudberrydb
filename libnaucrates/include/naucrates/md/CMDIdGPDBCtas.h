//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CMDIdGPDBCtas.h
//
//	@doc:
//		Class for representing mdids for GPDB CTAS queries
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------



#ifndef GPMD_CMDIdGPDBCTAS_H
#define GPMD_CMDIdGPDBCTAS_H

#include "gpos/base.h"

#include "naucrates/md/CMDIdGPDB.h"

namespace gpmd
{
	using namespace gpos;


	//---------------------------------------------------------------------------
	//	@class:
	//		CMDIdGPDBCtas
	//
	//	@doc:
	//		Class for representing ids of GPDB CTAS metadata objects
	//
	//---------------------------------------------------------------------------
	class CMDIdGPDBCtas : public CMDIdGPDB
	{
			
		public:
			// ctor
			explicit 
			CMDIdGPDBCtas(OID oid);
			
			// copy ctor
			explicit
			CMDIdGPDBCtas(const CMDIdGPDBCtas &mdidSource);

			// mdid type
			virtual
			EMDIdType Emdidt() const
			{
				return EmdidGPDBCtas;
			}
			
			// source system id
			virtual
			CSystemId Sysid() const
			{
				return m_sysid;
			}

			// equality check
			virtual
			BOOL FEquals(const IMDId *pmdid) const;
			
			// is the mdid valid
			virtual
			BOOL FValid() const;
						
			// debug print of the metadata id
			virtual
			IOstream &OsPrint(IOstream &os) const;
			
			// invalid mdid
			static 
			CMDIdGPDBCtas m_mdidInvalidKey;
			
			// const converter
			static
			const CMDIdGPDBCtas *PmdidConvert(const IMDId *pmdid)
			{
				GPOS_ASSERT(NULL != pmdid && EmdidGPDBCtas == pmdid->Emdidt());

				return dynamic_cast<const CMDIdGPDBCtas *>(pmdid);
			}
			
			// non-const converter
			static
			CMDIdGPDBCtas *PmdidConvert(IMDId *pmdid)
			{
				GPOS_ASSERT(NULL != pmdid && EmdidGPDBCtas == pmdid->Emdidt());

				return dynamic_cast<CMDIdGPDBCtas *>(pmdid);
			}
	};

}

#endif // !GPMD_CMDIdGPDBCTAS_H

// EOF
