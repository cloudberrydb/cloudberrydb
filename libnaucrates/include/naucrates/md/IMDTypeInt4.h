//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		IMDTypeInt4.h
//
//	@doc:
//		Interface for INT4 types in the metadata cache
//---------------------------------------------------------------------------

#ifndef GPMD_IMDTypeInt4_H
#define GPMD_IMDTypeInt4_H

#include "gpos/base.h"

#include "naucrates/md/IMDType.h"

// fwd decl
namespace gpnaucrates
{
	class IDatumInt4;
}

namespace gpmd
{
	using namespace gpos;
	using namespace gpnaucrates;

	
	//---------------------------------------------------------------------------
	//	@class:
	//		IMDTypeInt4
	//
	//	@doc:
	//		Interface for INT4 types in the metadata cache
	//
	//---------------------------------------------------------------------------
	class IMDTypeInt4 : public IMDType
	{
		public:
			// type id
			static ETypeInfo EtiType()
			{
				return EtiInt4;
			}
			
			virtual ETypeInfo Eti() const
			{
				return IMDTypeInt4::EtiType();
			} 
			
			// factory function for INT4 datums
			virtual IDatumInt4 *PdatumInt4(IMemoryPool *pmp, INT iValue, BOOL fNULL) const = 0;
		
	};

}

#endif // !GPMD_IMDTypeInt4_H

// EOF
