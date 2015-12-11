//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		IMDTypeInt2.h
//
//	@doc:
//		Interface for INT2 types in the metadata cache
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPMD_IMDTypeInt2_H
#define GPMD_IMDTypeInt2_H

#include "gpos/base.h"

#include "naucrates/md/IMDType.h"

// fwd decl
namespace gpnaucrates
{
	class IDatumInt2;
}

namespace gpmd
{
	using namespace gpos;
	using namespace gpnaucrates;

	
	//---------------------------------------------------------------------------
	//	@class:
	//		IMDTypeInt2
	//
	//	@doc:
	//		Interface for INT2 types in the metadata cache
	//
	//---------------------------------------------------------------------------
	class IMDTypeInt2 : public IMDType
	{
		public:
			// type id
			static ETypeInfo EtiType()
			{
				return EtiInt2;
			}
			
			virtual ETypeInfo Eti() const
			{
				return IMDTypeInt2::EtiType();
			} 
			
			// factory function for INT2 datums
			virtual IDatumInt2 *PdatumInt2(IMemoryPool *pmp, SINT sValue, BOOL fNULL) const = 0;
		
	};

}

#endif // !GPMD_IMDTypeInt2_H

// EOF
