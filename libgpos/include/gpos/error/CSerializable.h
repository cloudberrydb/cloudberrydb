//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CSerializable.h
//
//	@doc:
//		Interface for serializable objects;
//		used to dump objects when an exception is raised;
//---------------------------------------------------------------------------
#ifndef GPOS_CSerializable_H
#define GPOS_CSerializable_H

#include "gpos/base.h"
#include "gpos/common/CList.h"

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CSerializable
	//
	//	@doc:
	//		Interface for serializable objects;
	//
	//---------------------------------------------------------------------------
	class CSerializable : CStackObject
	{
		private:

			// private copy ctor
			CSerializable(const CSerializable&);

		public:

			// ctor
			CSerializable();

			// dtor
			virtual
			~CSerializable();

			// calculate space needed for serialization
			virtual
			ULONG_PTR UlpRequiredSpace() = 0;

			// serialize object to passed buffer
			virtual
			ULONG_PTR UlpSerialize(WCHAR *wszBuffer, ULONG_PTR ulpAllocSize) = 0;

			// link for list in error context
			SLink m_linkErrCtxt;

	}; // class CSerializable
}

#endif // !GPOS_CSerializable_H

// EOF

