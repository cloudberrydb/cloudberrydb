//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CMDIdGPDB.h
//
//	@doc:
//		Class for representing id and version of metadata objects in GPDB
//---------------------------------------------------------------------------



#ifndef GPMD_CMDIdGPDB_H
#define GPMD_CMDIdGPDB_H

#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/string/CWStringConst.h"

#include "naucrates/dxl/gpdb_types.h"

#include "naucrates/md/IMDId.h"
#include "naucrates/md/CSystemId.h"


// TODO:  - Feb 1, 2012; remove once system id is part of the mdid
#define GPMD_GPDB_SYSID GPOS_WSZ_LIT("GPDB")

namespace gpmd
{
	using namespace gpos;


	//---------------------------------------------------------------------------
	//	@class:
	//		CMDIdGPDB
	//
	//	@doc:
	//		Class for representing ids of GPDB metadata objects
	//
	//---------------------------------------------------------------------------
	class CMDIdGPDB : public IMDId
	{
		protected:
		
			// source system id
			CSystemId m_sysid;
			
			// id from GPDB system catalog
			OID m_oid;
			
			// major version number
			ULONG m_ulVersionMajor;
			
			// minor version number
			ULONG m_ulVersionMinor;
		
			// buffer for the serialized mdid
			WCHAR m_wszBuffer[GPDXL_MDID_LENGTH];
			
			// string representation of the mdid
			CWStringStatic m_str;
			
			// serialize mdid
			virtual
			void Serialize();
			
		public:
			// ctors
			CMDIdGPDB(CSystemId sysid, OID oid);
			explicit 
			CMDIdGPDB(OID oid);
			CMDIdGPDB(OID oid, ULONG ulVersionMajor, ULONG ulVersionMinor);
			
			// copy ctor
			explicit
			CMDIdGPDB(const CMDIdGPDB &mdidSource);

			virtual
			EMDIdType Emdidt() const
			{
				return EmdidGPDB;
			}
			
			// string representation of mdid
			virtual
			const WCHAR *Wsz() const;
			
			// source system id
			virtual
			CSystemId Sysid() const
			{
				return m_sysid;
			}
			
			// oid
			virtual
			OID OidObjectId() const;
			
			// major version
			virtual
			ULONG UlVersionMajor() const;
			
			// minor version
			virtual
			ULONG UlVersionMinor() const;

			// equality check
			virtual
			BOOL FEquals(const IMDId *pmdid) const;
			
			// computes the hash value for the metadata id
			virtual
			ULONG UlHash() const
			{
				return gpos::UlCombineHashes(Emdidt(),
						gpos::UlCombineHashes(gpos::UlHash(&m_oid),
											gpos::UlCombineHashes(gpos::UlHash(&m_ulVersionMajor), gpos::UlHash(&m_ulVersionMinor))));
			}
			
			// is the mdid valid
			virtual
			BOOL FValid() const;

			// serialize mdid in DXL as the value of the specified attribute 
			virtual
			void Serialize(CXMLSerializer *pxmlser, const CWStringConst *pstrAttribute) const;
						
			// debug print of the metadata id
			virtual
			IOstream &OsPrint(IOstream &os) const;
			
			// const converter
			static
			const CMDIdGPDB *PmdidConvert(const IMDId *pmdid)
			{
				GPOS_ASSERT(NULL != pmdid && EmdidGPDB == pmdid->Emdidt());

				return dynamic_cast<const CMDIdGPDB *>(pmdid);
			}
			
			// non-const converter
			static
			CMDIdGPDB *PmdidConvert(IMDId *pmdid)
			{
				GPOS_ASSERT(NULL != pmdid && (EmdidGPDB == pmdid->Emdidt() || EmdidGPDBCtas == pmdid->Emdidt()));

				return dynamic_cast<CMDIdGPDB *>(pmdid);
			}
			
			// invalid mdid
			static 
			CMDIdGPDB m_mdidInvalidKey;

			// int2 mdid
			static
			CMDIdGPDB m_mdidInt2;

			// int4 mdid
			static
			CMDIdGPDB m_mdidInt4;

			// int8 mdid
			static
			CMDIdGPDB m_mdidInt8;

			// oid mdid
			static
			CMDIdGPDB m_mdidOid;

			// bool mdid
			static
			CMDIdGPDB m_mdidBool;

			// numeric mdid
			static
			CMDIdGPDB m_mdidNumeric;

			// date mdid
			static
			CMDIdGPDB m_mdidDate;

			// time mdid
			static
			CMDIdGPDB m_mdidTime;

			// time with time zone mdid
			static
			CMDIdGPDB m_mdidTimeTz;

			// timestamp mdid
			static
			CMDIdGPDB m_mdidTimestamp;

			// timestamp with time zone mdid
			static
			CMDIdGPDB m_mdidTimestampTz;

			// absolute time mdid
			static
			CMDIdGPDB m_mdidAbsTime;

			// relative time mdid
			static
			CMDIdGPDB m_mdidRelTime;

			// interval mdid
			static
			CMDIdGPDB m_mdidInterval;

			// time interval mdid
			static
			CMDIdGPDB m_mdidTimeInterval;

			// bpchar mdid
			static
			CMDIdGPDB m_mdidBPChar;

			// varchar mdid
			static
			CMDIdGPDB m_mdidVarChar;

			// text mdid
			static
			CMDIdGPDB m_mdidText;

			// float4 mdid
			static
			CMDIdGPDB m_mdidFloat4;

			// float8 mdid
			static
			CMDIdGPDB m_mdidFloat8;

			// cash mdid
			static
			CMDIdGPDB m_mdidCash;

			// inet mdid
			static
			CMDIdGPDB m_mdidInet;

			// cidr mdid
			static
			CMDIdGPDB m_mdidCidr;

			// macaddr mdid
			static
			CMDIdGPDB m_mdidMacaddr;

			// count(*) mdid
			static
			CMDIdGPDB m_mdidCountStar;

			// count(Any) mdid
			static
			CMDIdGPDB m_mdidCountAny;

			// unknown datatype mdid
			static
			CMDIdGPDB m_mdidUnknown;
	};

}



#endif // !GPMD_CMDIdGPDB_H

// EOF
