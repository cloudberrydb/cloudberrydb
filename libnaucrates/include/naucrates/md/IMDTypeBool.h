//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		IMDTypeBool.h
//
//	@doc:
//		Interface for BOOL types in the metadata cache
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------



#ifndef GPMD_IMDTypeBool_H
#define GPMD_IMDTypeBool_H

#include "gpos/base.h"

#include "naucrates/dxl/gpdb_types.h"

#include "naucrates/md/IMDType.h"

// fwd decl
namespace gpnaucrates
{
	class IDatumBool;
}

namespace gpmd
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		IMDTypeBool
	//
	//	@doc:
	//		Interface for BOOL types in the metadata cache
	//
	//---------------------------------------------------------------------------
	class IMDTypeBool : public IMDType
	{
		public:
		
			// type id
			static ETypeInfo EtiType()
			{
				return EtiBool;
			}

			virtual ETypeInfo Eti() const
			{
				return IMDTypeBool::EtiType();
			}
			
			// factory function for BOOL datums
			virtual IDatumBool *PdatumBool(IMemoryPool *pmp, BOOL fValue, BOOL fNULL) const = 0;
		
	};

}

#endif // !GPMD_IMDTypeBool_H

// EOF
