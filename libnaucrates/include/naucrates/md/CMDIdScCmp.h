//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CMDIdScCmp.h
//
//	@doc:
//		Class for representing mdids of scalar comparison operators
//---------------------------------------------------------------------------

#ifndef GPMD_CMDIdScCmpFunc_H
#define GPMD_CMDIdScCmpFunc_H

#include "gpos/base.h"

#include "naucrates/md/IMDType.h"
#include "naucrates/md/CMDIdGPDB.h"

namespace gpmd
{
	using namespace gpos;


	//---------------------------------------------------------------------------
	//	@class:
	//		CMDIdScCmp
	//
	//	@doc:
	//		Class for representing ids of scalar comparison operators
	//
	//---------------------------------------------------------------------------
	class CMDIdScCmp : public IMDId
	{
		private:
			// mdid of source type
			CMDIdGPDB *m_pmdidLeft;
			
			// mdid of destinatin type
			CMDIdGPDB *m_pmdidRight;
	
			// comparison type
			IMDType::ECmpType m_ecmpt;
			
			// buffer for the serialized mdid
			WCHAR m_wszBuffer[GPDXL_MDID_LENGTH];
			
			// string representation of the mdid
			CWStringStatic m_str;
			
			// private copy ctor
			CMDIdScCmp(const CMDIdScCmp &);
			
			// serialize mdid
			void Serialize();
			
		public:
			// ctor
			CMDIdScCmp(CMDIdGPDB *pmdidLeft, CMDIdGPDB *pmdidRight, IMDType::ECmpType ecmpt);
			
			// dtor
			virtual
			~CMDIdScCmp();
			
			virtual
			EMDIdType Emdidt() const
			{
				return EmdidScCmp;
			}
			
			// string representation of mdid
			virtual
			const WCHAR *Wsz() const;
			
			// source system id
			virtual
			CSystemId Sysid() const
			{
				return m_pmdidLeft->Sysid();
			}
			
			// left type id
			IMDId *PmdidLeft() const;

			// right type id
			IMDId *PmdidRight() const;
			
			IMDType::ECmpType Ecmpt() const
			{ 
				return m_ecmpt;
			}
			
			// equality check
			virtual
			BOOL FEquals(const IMDId *pmdid) const;
			
			// computes the hash value for the metadata id
			virtual
			ULONG UlHash() const;
			
			// is the mdid valid
			virtual
			BOOL FValid() const
			{
				return IMDId::FValid(m_pmdidLeft) && IMDId::FValid(m_pmdidRight) && IMDType::EcmptOther != m_ecmpt;
			}

			// serialize mdid in DXL as the value of the specified attribute 
			virtual
			void Serialize(CXMLSerializer *pxmlser, const CWStringConst *pstrAttribute) const;
						
			// debug print of the metadata id
			virtual
			IOstream &OsPrint(IOstream &os) const;
			
			// const converter
			static
			const CMDIdScCmp *PmdidConvert(const IMDId *pmdid)
			{
				GPOS_ASSERT(NULL != pmdid && EmdidScCmp == pmdid->Emdidt());

				return dynamic_cast<const CMDIdScCmp *>(pmdid);
			}
			
			// non-const converter
			static
			CMDIdScCmp *PmdidConvert(IMDId *pmdid)
			{
				GPOS_ASSERT(NULL != pmdid && EmdidScCmp == pmdid->Emdidt());

				return dynamic_cast<CMDIdScCmp *>(pmdid);
			}
	};
}

#endif // !GPMD_CMDIdScCmpFunc_H

// EOF
