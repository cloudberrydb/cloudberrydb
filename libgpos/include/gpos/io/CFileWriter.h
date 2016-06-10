//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CFileWriter.h
//
//	@doc:
//		File writer
//---------------------------------------------------------------------------

#ifndef GPOS_CFileWriter_H
#define GPOS_CFileWriter_H

#include "gpos/io/CFileDescriptor.h"

namespace gpos
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CFileWriter
	//
	//	@doc:
	//		Implementation of file handler for raw output;
	//		does not provide thread-safety
	//
	//---------------------------------------------------------------------------
	class CFileWriter : public CFileDescriptor
	{
		private:

			// file size
			ULLONG m_ullSize;

			// no copy ctor
			CFileWriter(const CFileWriter &);

		public:

			// ctor
			CFileWriter();

			// dtor
			virtual
			~CFileWriter()
			{}

			ULLONG UllSize() const
			{
				return m_ullSize;
			}

			// open file for writing
			void Open(const CHAR *szPath, ULONG ulPerms);

			// close file
			void Close();

			// write bytes to file
			void Write(const BYTE *pb, const ULONG_PTR ullWriteSize);

	};	// class CFileWriter

}

#endif // !GPOS_CFileWriter_H

// EOF

