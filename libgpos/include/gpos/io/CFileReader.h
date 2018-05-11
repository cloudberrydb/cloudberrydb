//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CFileReader.h
//
//	@doc:
//		File Reader
//---------------------------------------------------------------------------

#ifndef GPOS_CFileReader_H
#define GPOS_CFileReader_H

#include <fcntl.h>
#include "gpos/io/CFileDescriptor.h"

namespace gpos
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CFileReader
	//
	//	@doc:
	//		Implementation of file handler for raw input;
	//		does not provide thread-safety
	//
	//---------------------------------------------------------------------------
	class CFileReader : public CFileDescriptor
	{
		private:

			// file size
			ULLONG m_ullSize;

			// read size
			ULLONG m_ullReadSize;

			// no copy ctor
			CFileReader(const CFileReader &);

		public:

			// ctor
			CFileReader();

			// dtor
			virtual
			~CFileReader();

			// get file size
			ULLONG UllSize() const;

			// get file read size
			ULLONG UllReadSize() const;

			// open file for reading
			void Open(const CHAR *szPath, const ULONG ulPerms =  S_IRUSR);

			// close file
			void Close();

			// read bytes to buffer
			ULONG_PTR UlpRead(BYTE *pb, const ULONG_PTR ulpReadSize);

	};	// class CFileReader

}

#endif // !GPOS_CFileReader_H

// EOF

