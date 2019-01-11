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
			const CWStringConst *PstrWindowFuncName(CMDAccessor *md_accessor, IMDId *mdid);

			// return the return type of the window operation
			static
			IMDId *PmdidWindowReturnType(CMDAccessor *md_accessor, IMDId *mdid);

			// does a cast object between given source and destination types exist
			static
			BOOL FCastExists(CMDAccessor *md_accessor, IMDId *mdid_src, IMDId *mdid_dest);

			// does a scalar comparison object between given types exist
			static
			BOOL FCmpExists(CMDAccessor *md_accessor, IMDId *left_mdid, IMDId *right_mdid, IMDType::ECmpType cmp_type);

			// is scalar operator commutative? this can be used with ScalarOp and ScalarCmp
			static
			BOOL FCommutativeScalarOp(CMDAccessor *md_accessor, IMDId *mdid_op);

			// does scalar operator return NULL on NULL input?
			static
			BOOL FScalarOpReturnsNullOnNullInput(CMDAccessor *md_accessor, IMDId *mdid_op);

			// return True if passed mdid is for BOOL type
			static
			BOOL FBoolType(CMDAccessor *md_accessor, IMDId *mdid_type);


	};
}

#endif // !GPOPT_CMDAccessorUtils_H

// EOF
