//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CMDIdColStats.h
//
//	@doc:
//		Class for representing mdids for column statistics
//---------------------------------------------------------------------------



#ifndef GPMD_CMDIdColStats_H
#define GPMD_CMDIdColStats_H

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
	//		CMDIdColStats
	//
	//	@doc:
	//		Class for representing ids of column stats objects
	//
	//---------------------------------------------------------------------------
	class CMDIdColStats : public IMDId
	{
		private:
			// mdid of base relation
			CMDIdGPDB *m_pmdidRel;
			
			// position of the attribute in the base relation
			ULONG m_ulPos;
			
			// buffer for the serialized mdid
			WCHAR m_wszBuffer[GPDXL_MDID_LENGTH];
			
			// string representation of the mdid
			CWStringStatic m_str;
			
			// private copy ctor
			CMDIdColStats(const CMDIdColStats &);
			
			// serialize mdid
			void Serialize();
			
		public:
			// ctor
			CMDIdColStats(CMDIdGPDB *pmdidRel, ULONG ulAttno);
			
			// dtor
			virtual
			~CMDIdColStats();
			
			virtual
			EMDIdType Emdidt() const
			{
				return EmdidColStats;
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
			ULONG UlPos() const;

			// equality check
			virtual
			BOOL FEquals(const IMDId *pmdid) const;
			
			// computes the hash value for the metadata id
			virtual
			ULONG UlHash() const
			{
				return gpos::UlCombineHashes(m_pmdidRel->UlHash(),
											gpos::UlHash(&m_ulPos));
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
			const CMDIdColStats *PmdidConvert(const IMDId *pmdid)
			{
				GPOS_ASSERT(NULL != pmdid && EmdidColStats == pmdid->Emdidt());

				return dynamic_cast<const CMDIdColStats *>(pmdid);
			}
			
			// non-const converter
			static
			CMDIdColStats *PmdidConvert(IMDId *pmdid)
			{
				GPOS_ASSERT(NULL != pmdid && EmdidColStats == pmdid->Emdidt());

				return dynamic_cast<CMDIdColStats *>(pmdid);
			}

	};

}



#endif // !GPMD_CMDIdColStats_H

// EOF
