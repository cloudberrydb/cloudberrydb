//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CXformContext.h
//
//	@doc:
//		Context container passed to every application of a transformation
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformContext_H
#define GPOPT_CXformContext_H

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"
#include "gpopt/operators/CPatternTree.h"

namespace gpopt
{
	using namespace gpos;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CXformContext
	//
	//	@doc:
	//		context container
	//
	//---------------------------------------------------------------------------
	class CXformContext : public CRefCount
	{

		private:

			// Memory pool
			IMemoryPool *m_mp;

			// private copy ctor
			CXformContext(const CXformContext &);

		public:
		
			// ctor
			explicit
			CXformContext
				(
				IMemoryPool *mp
				)
				: 
				m_mp(mp)
			{
			}

			// dtor
			~CXformContext() {}


			// accessor
			inline
			IMemoryPool *Pmp() const
			{
				return m_mp;
			}

	}; // class CXformContext

}


#endif // !GPOPT_CXformContext_H

// EOF
