//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CMDIdRelStats.h
//
//	@doc:
//		Class for representing mdids for relation statistics
//---------------------------------------------------------------------------



#ifndef GPMD_CMDIdRelStats_H
#define GPMD_CMDIdRelStats_H

#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/string/CWStringConst.h"

#include "naucrates/dxl/gpdb_types.h"

#include "naucrates/md/CMDIdGPDB.h"
#include "naucrates/md/CSystemId.h"

namespace gpmd
{
	using namespace gpos;


	//---------------------------------------------------------------------------
	//	@class:
	//		CMDIdRelStats
	//
	//	@doc:
	//		Class for representing ids of relation stats objects
	//
	//---------------------------------------------------------------------------
	class CMDIdRelStats : public IMDId
	{
		private:
		
			// mdid of base relation
			CMDIdGPDB *m_pmdidRel;
						
			// buffer for the serialzied mdid
			WCHAR m_wszBuffer[GPDXL_MDID_LENGTH];
			
			// string representation of the mdid
			CWStringStatic m_str;
			
			// private copy ctor
			CMDIdRelStats(const CMDIdRelStats &);
			
			// serialize mdid
			void Serialize();
			
		public:
			
			// ctor
			explicit
			CMDIdRelStats(CMDIdGPDB *pmdidRel);
			
			// dtor
			virtual
			~CMDIdRelStats();
			
			virtual
			EMDIdType Emdidt() const
			{
				return EmdidRelStats;
			}
			
			// string representation of mdid
			virtual
			const WCHAR *Wsz() const;
			
			// source system id
			virtual
			CSystemId Sysid() const
			{
				return m_pmdidRel->Sysid();
			}
			
			// accessors
			IMDId *PmdidRel() const;

			// equality check
			virtual
			BOOL FEquals(const IMDId *pmdid) const;
			
			// computes the hash value for the metadata id
			virtual
			ULONG UlHash() const
			{
				return m_pmdidRel->UlHash();
			}
			
			// is the mdid valid
			virtual
			BOOL FValid() const
			{
				return IMDId::FValid(m_pmdidRel);
			}

			// serialize mdid in DXL as the value of the specified attribute 
			virtual
			void Serialize(CXMLSerializer *pxmlser, const CWStringConst *pstrAttribute) const;
						
			// debug print of the metadata id
			virtual
			IOstream &OsPrint(IOstream &os) const;
			
			// const converter
			static
			const CMDIdRelStats *PmdidConvert(const IMDId *pmdid)
			{
				GPOS_ASSERT(NULL != pmdid && EmdidRelStats == pmdid->Emdidt());

				return dynamic_cast<const CMDIdRelStats *>(pmdid);
			}
			
			// non-const converter
			static
			CMDIdRelStats *PmdidConvert(IMDId *pmdid)
			{
				GPOS_ASSERT(NULL != pmdid && EmdidRelStats == pmdid->Emdidt());

				return dynamic_cast<CMDIdRelStats *>(pmdid);
			}

	};

}



#endif // !GPMD_CMDIdRelStats_H

// EOF
