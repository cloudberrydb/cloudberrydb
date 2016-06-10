//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		IMDScCmp.h
//
//	@doc:
//		Interface for scalar comparison operators in the MD cache
//---------------------------------------------------------------------------



#ifndef GPMD_IMDScCmp_H
#define GPMD_IMDScCmp_H

#include "gpos/base.h"

#include "naucrates/md/IMDCacheObject.h"
#include "naucrates/md/IMDType.h"

namespace gpmd
{
	using namespace gpos;

	
	//---------------------------------------------------------------------------
	//	@class:
	//		IMDScCmp
	//
	//	@doc:
	//		Interface for scalar comparison operators in the MD cache
	//
	//---------------------------------------------------------------------------
	class IMDScCmp : public IMDCacheObject
	{	
		public:

			// object type
			virtual
			Emdtype Emdt() const
			{
				return EmdtScCmp;
			}

			// left type
			virtual 
			IMDId *PmdidLeft() const = 0;

			// right type
			virtual
			IMDId *PmdidRight() const = 0;
			
			// comparison type
			virtual 
			IMDType::ECmpType Ecmpt() const = 0;

			// comparison operator id
			virtual 
			IMDId *PmdidOp() const = 0;
	};
		
}

#endif // !GPMD_IMDScCmp_H

// EOF
