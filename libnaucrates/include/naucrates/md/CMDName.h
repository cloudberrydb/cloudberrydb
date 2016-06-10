//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CMDName.h
//
//	@doc:
//		Class for representing metadata names.
//---------------------------------------------------------------------------

#ifndef GPMD_CMDName_H
#define GPMD_CMDName_H

#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/string/CWStringConst.h"

namespace gpmd
{	
	using namespace gpos;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CMDName
	//
	//	@doc:
	//		Class for representing metadata names.
	//
	//---------------------------------------------------------------------------
	class CMDName
	{
		private:
			// the string holding the name
			const CWStringConst *m_psc;
			
			// keep track of copy status
			BOOL m_fDeepCopy;
		
		public:
			// ctor/dtor
			CMDName(IMemoryPool *pmp, const CWStringBase *pstr);
			CMDName(const CWStringConst *, BOOL fOwnsMemory = false);
			
			// shallow copy ctor
			CMDName(const CMDName &);
			
			~CMDName();

			// accessors
			const CWStringConst *Pstr() const
			{
				return m_psc;
			}
	};

	// array of names
	typedef CDynamicPtrArray<CMDName, CleanupDelete> DrgPmdname;
}

#endif // !GPMD_CMDName_H

// EOF
