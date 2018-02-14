//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal Software, Inc.
//
//	@filename:
//		CMDArrayCoerceCastGPDB.h
//
//	@doc:
//		Implementation of GPDB-specific array coerce cast functions in the
//		metadata cache
//---------------------------------------------------------------------------


#ifndef GPMD_CMDArrayCoerceCastGPDB_H
#define GPMD_CMDArrayCoerceCastGPDB_H

#include "gpos/base.h"

#include "naucrates/md/CMDCastGPDB.h"
#include "naucrates/dxl/operators/CDXLScalar.h"

namespace gpmd
{

	using namespace gpdxl;

	class CMDArrayCoerceCastGPDB : public CMDCastGPDB
	{
		private:
			// DXL for object
			const CWStringDynamic *m_pstr;

			// type mod
			INT m_iTypeModifier;

			// is explicit
			BOOL m_fIsExplicit;

			// CoercionForm
			EdxlCoercionForm m_edxlcf;

			// iLoc
			INT m_iLoc;

			// private copy ctor
			CMDArrayCoerceCastGPDB(const CMDArrayCoerceCastGPDB &);

		public:
			// ctor
			CMDArrayCoerceCastGPDB
				(
				IMemoryPool *pmp,
				IMDId *pmdid,
				CMDName *pmdname,
				IMDId *pmdidSrc,
				IMDId *pmdidDest,
				BOOL fBinaryCoercible,
				IMDId *pmdidCastFunc,
				EmdCoercepathType emdPathType,
				INT iTypeModifier,
				BOOL fIsExplicit,
				EdxlCoercionForm edxlcf,
				INT iLoc
				);

			// dtor
			virtual
			~CMDArrayCoerceCastGPDB();

			// accessors
			virtual
			const CWStringDynamic *Pstr() const
			{
				return m_pstr;
			}

			// return type modifier
			virtual
			INT ITypeModifier() const;

			virtual
			BOOL FIsExplicit() const;

			// return coercion form
			virtual
			EdxlCoercionForm Ecf() const;

			// return token location
			virtual
			INT ILoc() const;

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

#endif // !GPMD_CMDArrayCoerceCastGPDB_H

// EOF
