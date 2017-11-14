//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CReqdProp.h
//
//	@doc:
//		Base class for all required properties
//---------------------------------------------------------------------------
#ifndef GPOPT_CReqdProp_H
#define GPOPT_CReqdProp_H

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"
#include "gpos/common/CDynamicPtrArray.h"

#include "gpopt/base/CDrvdProp.h"

namespace gpopt
{
	using namespace gpos;

	// forward declarations
	class CExpressionHandle;
	class COperator;
	class CReqdProp;

	// dynamic array for required properties
	typedef CDynamicPtrArray<CReqdProp, CleanupRelease> DrgPrp;

	//---------------------------------------------------------------------------
	//	@class:
	//		CReqdProp
	//
	//	@doc:
	//		Abstract base class for all required properties. Individual property
	//		components are added separately. CReqdProp is memory pool-agnostic.
	//
	//---------------------------------------------------------------------------
	class CReqdProp : public CRefCount
	{

		public:

			// types of required properties
			enum EPropType
			{
				EptRelational,
				EptPlan,

				EptSentinel
			};

		private:

			// private copy ctor
			CReqdProp(const CReqdProp &);

		public:

			// ctor
			CReqdProp();

			// dtor
			virtual
			~CReqdProp();

			// is it a relational property?
			virtual
			BOOL FRelational() const
			{
				return false;
			}

			// is it a plan property?
			virtual
			BOOL FPlan() const
			{
				return false;
			}

			// required properties computation function
			virtual
			void Compute
				(
				IMemoryPool *pmp,
				CExpressionHandle &exprhdl,
				CReqdProp *prpInput,
				ULONG ulChildIndex,
				DrgPdp *pdrgpdpCtxt,
				ULONG ulOptReq
				) = 0;

			// print function
			virtual
			IOstream &OsPrint(IOstream &os) const = 0;

#ifdef GPOS_DEBUG
			// debug print for interactive debugging sessions only
			void DbgPrint() const;
#endif // GPOS_DEBUG

	}; // class CReqdProp

	// shorthand for printing
	inline
	IOstream &operator << (IOstream &os, const CReqdProp &reqdprop)
	{
		return reqdprop.OsPrint(os);
	}

}


#endif // !GPOPT_CReqdProp_H

// EOF
