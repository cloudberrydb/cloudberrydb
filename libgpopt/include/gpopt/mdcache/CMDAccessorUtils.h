//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CMDAccessorUtils.h
//
//	@doc:
//		Utility functions associated with the metadata cache accessor
//---------------------------------------------------------------------------



#ifndef GPOPT_CMDAccessorUtils_H
#define GPOPT_CMDAccessorUtils_H

#include "gpopt/mdcache/CMDAccessor.h"

namespace gpopt
{
	using namespace gpos;
	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		CMDAccessorUtils
	//
	//	@doc:
	//		Utility functions associated with the metadata cache accessor
	//
	//---------------------------------------------------------------------------
	class CMDAccessorUtils
	{
		public:

			// return the name of the window operation
			static
			const CWStringConst *PstrWindowFuncName(CMDAccessor *pmda, IMDId *pmdid);

			// return the return type of the window operation
			static
			IMDId *PmdidWindowReturnType(CMDAccessor *pmda, IMDId *pmdid);

			// does a cast object between given source and destination types exist
			static
			BOOL FCastExists(CMDAccessor *pmda, IMDId *pmdidSrc, IMDId *pmdidDest);

			// does a scalar comparison object between given types exist
			static
			BOOL FCmpExists(CMDAccessor *pmda, IMDId *pmdidLeft, IMDId *pmdidRight, IMDType::ECmpType ecmpt);

			// is scalar operator commutative? this can be used with ScalarOp and ScalarCmp
			static
			BOOL FCommutativeScalarOp(CMDAccessor *pmda, IMDId *pmdidOp);

			// does scalar operator return NULL on NULL input?
			static
			BOOL FScalarOpReturnsNullOnNullInput(CMDAccessor *pmda, IMDId *pmdidOp);

			// return True if passed mdid is for BOOL type
			static
			BOOL FBoolType(CMDAccessor *pmda, IMDId *pmdidType);


	};
}

#endif // !GPOPT_CMDAccessorUtils_H

// EOF
