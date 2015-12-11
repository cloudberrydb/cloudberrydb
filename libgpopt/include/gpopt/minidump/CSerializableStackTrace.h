//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CSerializableStackTrace.h
//
//	@doc:
//		Serializable stack trace object
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CSerializableStackTrace_H
#define GPOPT_CSerializableStackTrace_H

#include "gpos/base.h"
#include "gpos/error/CSerializable.h"
#include "gpos/string/CWStringDynamic.h"

#include "naucrates/dxl/operators/CDXLNode.h"

using namespace gpos;
using namespace gpdxl;


namespace gpopt
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CSerializableStackTrace
	//
	//	@doc:
	//		Serializable stacktrace object 
	//
	//---------------------------------------------------------------------------
	class CSerializableStackTrace : public CSerializable
	{
		private:

			// buffer used for creating the stack trace
			WCHAR *m_wszBuffer;

			// was buffer successfully allocated?
			BOOL m_fAllocated;

		public:

			// ctor
			CSerializableStackTrace();

			// dtor
			virtual
			~CSerializableStackTrace();

			// allocate buffer
			void AllocateBuffer(IMemoryPool *pmp);

			// calculate space needed for serialization
			virtual
			ULONG_PTR UlpRequiredSpace();

			// serialize object to passed buffer
			virtual
			ULONG_PTR UlpSerialize(WCHAR *wszBuffer, ULONG_PTR ulpAllocSize);

	}; // class CSerializableStackTrace
}

#endif // !GPOPT_CSerializableStackTrace_H

// EOF

