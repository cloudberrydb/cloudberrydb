//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMiniDumperDXL.h
//
//	@doc:
//		DXL-based minidump handler;
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CMiniDumperDXL_H
#define GPOPT_CMiniDumperDXL_H

#include "gpos/base.h"
#include "gpos/error/CMiniDumper.h"

using namespace gpos;

namespace gpopt
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CMiniDumperDXL
	//
	//	@doc:
	//		DXL-specific minidump handler
	//
	//---------------------------------------------------------------------------
	class CMiniDumperDXL : public CMiniDumper
	{
		private:			
			// private copy ctor
			CMiniDumperDXL(const CMiniDumperDXL&);

		public:

			// ctor
			explicit
			CMiniDumperDXL(IMemoryPool *pmp);

			// dtor
			virtual
			~CMiniDumperDXL();

			// serialize minidump header
			virtual
			void SerializeHeader();

			// serialize minidump footer
			virtual
			void SerializeFooter();

			// size to reserve for entry header
			virtual
			ULONG_PTR UlpRequiredSpaceEntryHeader();

			// size to reserve for entry header
			virtual
			ULONG_PTR UlpRequiredSpaceEntryFooter();

			// serialize entry header
			virtual
			ULONG_PTR UlpSerializeEntryHeader(WCHAR *wszEntry, ULONG_PTR ulpAllocSize);

			// serialize entry footer
			virtual
			ULONG_PTR UlpSerializeEntryFooter(WCHAR *wszEntry, ULONG_PTR ulpAllocSize);

	}; // class CMiniDumperDXL

	// shorthand for printing
	inline
	IOstream &operator << (IOstream &os, const CMiniDumperDXL &mdr)
	{
		return mdr.OsPrint(os);
	}
}

#endif // !GPOPT_CMiniDumperDXL_H

// EOF

