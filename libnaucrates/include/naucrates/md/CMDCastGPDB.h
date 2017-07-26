//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CMDCastGPDB.h
//
//	@doc:
//		Implementation of GPDB-specific cast functions in the metadata cache
//---------------------------------------------------------------------------


#ifndef GPMD_CMDCastGPDB_H
#define GPMD_CMDCastGPDB_H

#include "gpos/base.h"

#include "naucrates/md/IMDCast.h"

namespace gpmd
{
	
	using namespace gpdxl;

	//---------------------------------------------------------------------------
	//	@class:
	//		CMDCastGPDB
	//
	//	@doc:
	//		Implementation for GPDB-specific cast functions in the metadata cache
	//
	//---------------------------------------------------------------------------
	class CMDCastGPDB : public IMDCast
	{
		private:
			// private copy ctor
			CMDCastGPDB(const CMDCastGPDB &);

		protected:
			// memory pool
			IMemoryPool *m_pmp;
			
			// DXL for object
			const CWStringDynamic *m_pstr;
			
			// func id
			IMDId *m_pmdid;
			
			// func name
			CMDName *m_pmdname;
			
			// source type
			IMDId *m_pmdidSrc;
			
			// destination type
			IMDId *m_pmdidDest;
			
			// is cast between binary coercible types, i.e. the types are binary compatible
			BOOL m_fBinaryCoercible;
			
			// cast func id
			IMDId *m_pmdidCastFunc;

			// coercion path type
			EmdCoercepathType m_emdPathType;
			
		public:
			// ctor
			CMDCastGPDB
				(
				IMemoryPool *pmp,
				IMDId *pmdid,
				CMDName *pmdname,
				IMDId *pmdidSrc,
				IMDId *pmdidDest,
				BOOL fBinaryCoercible,
				IMDId *pmdidCastFunc,
				EmdCoercepathType emdPathType = EmdtNone
				);
			
			// dtor
			virtual
			~CMDCastGPDB();
			
			// accessors
			virtual 
			const CWStringDynamic *Pstr() const
			{
				return m_pstr;
			}
			
			// cast object id
			virtual 
			IMDId *Pmdid() const;
			
			// cast object name
			virtual 
			CMDName Mdname() const;
			
			// source type
			virtual 
			IMDId *PmdidSrc() const;

			// destination type
			virtual 
			IMDId *PmdidDest() const;
			
			// is this a cast between binary coeercible types, i.e. the types are binary compatible
			virtual 
			BOOL FBinaryCoercible() const;

			// return the coercion path type
			virtual
			EmdCoercepathType EmdPathType() const;

			// cast function id
			virtual 
			IMDId *PmdidCastFunc() const;
		
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

#endif // !GPMD_CMDCastGPDB_H

// EOF
