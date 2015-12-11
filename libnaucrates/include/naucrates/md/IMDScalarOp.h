//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		IMDScalarOp.h
//
//	@doc:
//		Interface for scalar operators in the metadata cache
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------



#ifndef GPMD_IMDScalarOp_H
#define GPMD_IMDScalarOp_H

#include "gpos/base.h"

#include "naucrates/md/IMDCacheObject.h"
#include "naucrates/md/IMDType.h"

namespace gpmd
{
	using namespace gpos;

	
	//---------------------------------------------------------------------------
	//	@class:
	//		IMDScalarOp
	//
	//	@doc:
	//		Interface for scalar operators in the metadata cache
	//
	//---------------------------------------------------------------------------
	class IMDScalarOp : public IMDCacheObject
	{		
		public:
			
			// object type
			virtual
			Emdtype Emdt() const
			{
				return EmdtOp;
			}
		
			// type of left operand
			virtual 
			IMDId *PmdidTypeLeft() const = 0;
			
			// type of right operand
			virtual 
			IMDId *PmdidTypeRight() const = 0;

			// type of result operand
			virtual 
			IMDId *PmdidTypeResult() const = 0;
			
			// id of function which implements the operator
			virtual 
			IMDId *PmdidFunc() const = 0;
						
			// id of commute operator
			virtual 
			IMDId *PmdidOpCommute() const = 0;
			
			// id of inverse operator
			virtual 
			IMDId *PmdidOpInverse() const = 0;
			
			// is this an equality operator
			virtual 
			BOOL FEquality() const = 0;
			
			// does operator return NULL when all inputs are NULL?
			virtual
			BOOL FReturnsNullOnNullInput() const = 0;

			virtual
			IMDType::ECmpType Ecmpt() const = 0;

			// operator name
			virtual
			CMDName Mdname() const = 0;
			
			// number of classes this operator belongs to
			virtual
			ULONG UlOpCLasses() const = 0;
			
			// operator class at given position
			virtual
			IMDId *PmdidOpClass(ULONG ulPos) const = 0;
	};
}

#endif // !GPMD_IMDScalarOp_H

// EOF
