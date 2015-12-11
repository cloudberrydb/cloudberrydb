//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		IMDTypeInt8.h
//
//	@doc:
//		Interface for INT8 types in the metadata cache
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPMD_IMDTypeInt8_H
#define GPMD_IMDTypeInt8_H

#include "gpos/base.h"

#include "naucrates/md/IMDType.h"

// fwd decl
namespace gpnaucrates
{
	class IDatumInt8;
}

namespace gpmd
{
	using namespace gpos;
	using namespace gpnaucrates;


	//---------------------------------------------------------------------------
	//	@class:
	//		IMDTypeInt8
	//
	//	@doc:
	//		Interface for INT8 types in the metadata cache
	//
	//---------------------------------------------------------------------------
	class IMDTypeInt8 : public IMDType
	{
		public:
			// type id
			static ETypeInfo EtiType()
			{
				return EtiInt8;
			}

			virtual ETypeInfo Eti() const
			{
				return IMDTypeInt8::EtiType();
			}

			// factory function for INT8 datums
			virtual IDatumInt8 *PdatumInt8(IMemoryPool *pmp, LINT ulValue, BOOL fNULL) const = 0;

	};

}

#endif // !GPMD_IMDTypeInt8_H

// EOF
