//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		IMDFunction.h
//
//	@doc:
//		Interface for functions in the metadata cache
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------



#ifndef GPMD_IMDFunction_H
#define GPMD_IMDFunction_H

#include "gpos/base.h"

#include "naucrates/md/IMDCacheObject.h"

namespace gpmd
{
	using namespace gpos;

	
	//---------------------------------------------------------------------------
	//	@class:
	//		IMDFunction
	//
	//	@doc:
	//		Interface for functions in the metadata cache
	//
	//---------------------------------------------------------------------------
	class IMDFunction : public IMDCacheObject
	{	
		public:

			// function stability property
			enum EFuncStbl
			{
				EfsImmutable, /* never changes for given input */
				EfsStable, /* does not change within a scan */
				EfsVolatile, /* can change even within a scan */
				EfsSentinel
			};

			// function data access property
			enum EFuncDataAcc
			{
				EfdaNoSQL,
				EfdaContainsSQL,
				EfdaReadsSQLData,
				EfdaModifiesSQLData,
				EfdaSentinel
			};

			// object type
			virtual
			Emdtype Emdt() const
			{
				return EmdtFunc;
			}

			// does function return NULL on NULL input
			virtual 
			BOOL FStrict() const = 0;
			
			// does function return a set of values
			virtual 
			BOOL FReturnsSet() const = 0;

			// function stability
			virtual
			EFuncStbl EfsStability() const = 0;

			// function data access
			virtual
			EFuncDataAcc EfdaDataAccess() const = 0;

			// result type
			virtual 
			IMDId *PmdidTypeResult() const = 0;
			
			// output argument types
			virtual
			DrgPmdid *PdrgpmdidOutputArgTypes() const = 0;
	};
		
}

#endif // !GPMD_IMDFunction_H

// EOF
