//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC CORP.
//
//	@filename:
//		CDrvdPropCtxtRelational.h
//
//	@doc:
//		Container of information passed among expression nodes during
//		derivation of relational properties
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CDrvdPropCtxtRelational_H
#define GPOPT_CDrvdPropCtxtRelational_H

#include "gpos/base.h"

#include "gpopt/base/CDrvdPropCtxt.h"


namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDrvdPropCtxtRelational
	//
	//	@doc:
	//		Container of information passed among expression nodes during
	//		derivation of relational properties
	//
	//---------------------------------------------------------------------------
	class CDrvdPropCtxtRelational : public CDrvdPropCtxt
	{

		private:

			// private copy ctor
			CDrvdPropCtxtRelational(const CDrvdPropCtxtRelational &);

		protected:

			// copy function
			virtual
			CDrvdPropCtxt *PdpctxtCopy
				(
				IMemoryPool *pmp
				)
				const
			{
				return GPOS_NEW(pmp) CDrvdPropCtxtRelational(pmp);
			}

			// add props to context
			virtual
			void AddProps
				(
				CDrvdProp* // pdp
				)
			{
				// derived relational context is currently empty
			}

		public:

			// ctor
			CDrvdPropCtxtRelational
				(
				IMemoryPool *pmp
				)
				:
				CDrvdPropCtxt(pmp)
			{}

			// dtor
			virtual
			~CDrvdPropCtxtRelational() {}

			// print
			virtual
			IOstream &OsPrint
				(
				IOstream &os
				)
				const
			{
				return os;
			}

#ifdef GPOS_DEBUG

			// is it a relational property context?
			virtual
			BOOL FRelational() const
			{
				return true;
			}

#endif // GPOS_DEBUG

			// conversion function
			static
			CDrvdPropCtxtRelational *PdpctxtrelConvert
				(
				CDrvdPropCtxt *pdpctxt
				)
			{
				GPOS_ASSERT(NULL != pdpctxt);

				return reinterpret_cast<CDrvdPropCtxtRelational*>(pdpctxt);
			}

	}; // class CDrvdPropCtxtRelational

}


#endif // !GPOPT_CDrvdPropCtxtRelational_H

// EOF
