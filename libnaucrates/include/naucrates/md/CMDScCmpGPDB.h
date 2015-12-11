//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CMDScCmpGPDB.h
//
//	@doc:
//		Implementation of GPDB-specific scalar comparison operators in the MD cache
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------


#ifndef GPMD_CMDScCmpGPDB_H
#define GPMD_CMDScCmpGPDB_H

#include "gpos/base.h"

#include "naucrates/md/IMDScCmp.h"

namespace gpmd
{
	
	using namespace gpdxl;

	//---------------------------------------------------------------------------
	//	@class:
	//		CMDScCmpGPDB
	//
	//	@doc:
	//		Implementation for GPDB-specific scalar comparison operators in the 
	//		MD cache
	//
	//---------------------------------------------------------------------------
	class CMDScCmpGPDB : public IMDScCmp
	{
		private:
			// memory pool
			IMemoryPool *m_pmp;
			
			// DXL for object
			const CWStringDynamic *m_pstr;
			
			// object id
			IMDId *m_pmdid;
			
			// operator name
			CMDName *m_pmdname;
			
			// left type
			IMDId *m_pmdidLeft;
			
			// right type
			IMDId *m_pmdidRight;
			
			// comparison type
			IMDType::ECmpType m_ecmpt;
			
			// comparison operator id
			IMDId *m_pmdidOp;
			
			// private copy ctor
			CMDScCmpGPDB(const CMDScCmpGPDB &);
			
		public:
			// ctor
			CMDScCmpGPDB
				(
				IMemoryPool *pmp,
				IMDId *pmdid,
				CMDName *pmdname,
				IMDId *pmdidLeft,
				IMDId *pmdidRight,
				IMDType::ECmpType ecmpt,
				IMDId *pmdidOp
				);
			
			// dtor
			virtual
			~CMDScCmpGPDB();
			
			// accessors
			virtual 
			const CWStringDynamic *Pstr() const
			{
				return m_pstr;
			}
			
			// copmarison object id
			virtual 
			IMDId *Pmdid() const;
			
			// cast object name
			virtual 
			CMDName Mdname() const;
			
			// left type
			virtual 
			IMDId *PmdidLeft() const;

			// right type
			virtual 
			IMDId *PmdidRight() const;
			
			// comparison type
			virtual 
			IMDType::ECmpType Ecmpt() const;
			
			// comparison operator id
			virtual 
			IMDId *PmdidOp() const;
		
			// serialize object in DXL format
			virtual 
			void Serialize(gpdxl::CXMLSerializer *pxmlser) const;
			
#ifdef GPOS_DEBUG
			// debug print of the type in the provided stream
			virtual 
			void DebugPrint(IOstream &os) const;
#endif
	};
}

#endif // !GPMD_CMDScCmpGPDB_H

// EOF
