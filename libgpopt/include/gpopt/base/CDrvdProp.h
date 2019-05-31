//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 - 2011 EMC CORP.
//
//	@filename:
//		DrvdPropArray.h
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
	class DrvdPropArray;
	class CDrvdPropCtxt;
	class CReqdPropPlan;
	
	// dynamic array for properties
	typedef CDynamicPtrArray<DrvdPropArray, CleanupRelease> CDrvdProp2dArray;

	//---------------------------------------------------------------------------
	//	@class:
	//		DrvdPropArray
	//
	//	@doc:
	//		Abstract base class for all derived properties. Individual property
	//		components are added separately. DrvdPropArray is memory pool-agnostic.
	//
	//		All derived property classes implement a pure virtual function
	//		DrvdPropArray::Derive(). This function is responsible for filling in the
	//		different properties in the property container. For example
	//		CDrvdPropScalar::Derive() fills in used and defined columns in the
	//		current scalar property container.
	//
	//		Property derivation takes place in a bottom-up fashion. Each operator
	//		has to implement virtual derivation functions to be called by the
	//		derivation mechanism of each single property. For example,
	//		CPhysical::PosDerive() is used to derive sort order of an expression
	//		rooted by a given physical operator. Similarly, CScalar::PcrsUsed() is
	//		used to derive the used columns in a scalar expression rooted by a
	//		given operator.
	//
	//		The derivation functions take as argument a CExpressionHandle object,
	//		which is an abstraction of the child nodes (which could be Memo groups,
	//		or actual operators in stand-alone expression trees). This gives the
	//		derivation functions a unified way to access the properties of the
	//		children and combine them with local properties.
	//
	//		The derivation mechanism is kicked off by the function
	//		CExpressionHandle::DeriveProps().
	//
	//---------------------------------------------------------------------------
	class DrvdPropArray : public CRefCount
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
			DrvdPropArray(const DrvdPropArray &);

		public:

			// ctor
			DrvdPropArray();

			// dtor
			virtual 
			~DrvdPropArray() {}

			// type of properties
			virtual
			EPropType Ept() = 0;

			// derivation function
			virtual
			void Derive(CMemoryPool *mp, CExpressionHandle &exprhdl, CDrvdPropCtxt *pdppropctxt) = 0;

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

	}; // class DrvdPropArray

 	// shorthand for printing
	IOstream &operator << (IOstream &os, const DrvdPropArray &drvdprop);

}


#endif // !GPOPT_CDrvdProp_H

// EOF
