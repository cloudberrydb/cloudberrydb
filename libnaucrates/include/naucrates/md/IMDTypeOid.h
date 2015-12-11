//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		IMDTypeOid.h
//
//	@doc:
//		Interface for OID types in the metadata cache
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPMD_IMDTypeOid_H
#define GPMD_IMDTypeOid_H

#include "gpos/base.h"

#include "naucrates/md/IMDType.h"

// fwd decl
namespace gpnaucrates
{
	class IDatumOid;
}

namespace gpmd
{
	using namespace gpos;
	using namespace gpnaucrates;


	//---------------------------------------------------------------------------
	//	@class:
	//		IMDTypeOid
	//
	//	@doc:
	//		Interface for OID types in the metadata cache
	//
	//---------------------------------------------------------------------------
	class IMDTypeOid : public IMDType
	{
		public:
			// type id
			static
			ETypeInfo EtiType()
			{
				return EtiOid;
			}

			virtual
			ETypeInfo Eti() const
			{
				return IMDTypeOid::EtiType();
			}

			// factory function for OID datums
			virtual
			IDatumOid *PdatumOid(IMemoryPool *pmp, OID oidValue, BOOL fNull) const = 0;
	};
}

#endif // !GPMD_IMDTypeOid_H

// EOF
