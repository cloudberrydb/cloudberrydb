//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 - 2011 EMC CORP.
//
//	@filename:
//		CDrvdProp.h
//
//	@doc:
//		Base class for all derived properties
//---------------------------------------------------------------------------
#ifndef GPOPT_CDrvdProp_H
#define GPOPT_CDrvdProp_H

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"
#include "gpos/common/CDynamicPtrArray.h"


namespace gpopt
{
	using namespace gpos;
	
	// fwd declarations
	class CExpressionHandle;
	class COperator;
	class CDrvdProp;
	class CDrvdPropCtxt;
	class CReqdPropPlan;
	
	// dynamic array for properties
	typedef CDynamicPtrArray<CDrvdProp, CleanupRelease> DrgPdp;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDrvdProp
	//
	//	@doc:
	//		Abstract base class for all derived properties. Individual property
	//		components are added separately. CDrvdProp is memory pool-agnostic.
	//
	//---------------------------------------------------------------------------
	class CDrvdProp : public CRefCount
	{

		public:

			// types of derived properties
			enum EPropType
			{
				EptRelational,
				EptPlan,
				EptScalar,

				EptInvalid,
				EptSentinel = EptInvalid
			};

		private:

			// private copy ctor
			CDrvdProp(const CDrvdProp &);

		public:

			// ctor
			CDrvdProp();

			// dtor
			virtual 
			~CDrvdProp() {}

			// type of properties
			virtual
			EPropType Ept() = 0;

			// derivation function
			virtual
			void Derive(IMemoryPool *pmp, CExpressionHandle &exprhdl, CDrvdPropCtxt *pdppropctxt) = 0;

			// check for satisfying required plan properties
			virtual
			BOOL FSatisfies(const CReqdPropPlan *prpp) const = 0;

			// print
			virtual
			IOstream &OsPrint(IOstream &os) const = 0;

#ifdef GPOS_DEBUG
			// debug print for interactive debugging sessions only
			void DbgPrint() const;
#endif // GPOS_DEBUG

	}; // class CDrvdProp

 	// shorthand for printing
	IOstream &operator << (IOstream &os, const CDrvdProp &drvdprop);

}


#endif // !GPOPT_CDrvdProp_H

// EOF
