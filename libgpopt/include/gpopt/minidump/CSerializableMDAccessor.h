//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CSerializableMDAccessor.h
//
//	@doc:
//		Wrapper for serializing MD accessor objects
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOS_CSerializableMDAccessor_H
#define GPOS_CSerializableMDAccessor_H

#include "gpos/base.h"
#include "gpos/error/CSerializable.h"

using namespace gpos;
using namespace gpdxl;

namespace gpopt
{
	// fwd decl
	class CMDAccessor;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CSerializableMDAccessor
	//
	//	@doc:
	//		Wrapper for serializing MD objects in a minidump 
	//
	//---------------------------------------------------------------------------
	class CSerializableMDAccessor : public CSerializable
	{
		private:
			// MD accessor
			CMDAccessor *m_pmda;
			
			// serialize header
			ULONG_PTR UlpSerializeHeader(WCHAR *wszBuffer, ULONG_PTR ulpAllocSize);
			
			// serialize footer
			ULONG_PTR UlpSerializeFooter(WCHAR *wszBuffer, ULONG_PTR ulpAllocSize);

			// private copy ctor
			CSerializableMDAccessor(const CSerializableMDAccessor&);

		public:

			// ctor
			explicit
			CSerializableMDAccessor(CMDAccessor *pmda);

			// dtor
			virtual
			~CSerializableMDAccessor()
			{}

			// calculate space needed for serialization
			virtual
			ULONG_PTR UlpRequiredSpace();
			
			// serialize object to passed buffer
			virtual
			ULONG_PTR UlpSerialize(WCHAR *wszBuffer, ULONG_PTR ulpAllocSize);

	}; // class CSerializableMDAccessor
}

#endif // !GPOS_CSerializableMDAccessor_H

// EOF

