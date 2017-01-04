//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CFileDescriptor.h
//
//	@doc:
//		File descriptor abstraction
//---------------------------------------------------------------------------
#ifndef GPOS_CFileDescriptor_H
#define GPOS_CFileDescriptor_H

#include "gpos/types.h"

#define GPOS_FILE_NAME_BUF_SIZE   (1024)
#define GPOS_FILE_DESCR_INVALID   (-1)

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CFileDescriptor
	//
	//	@doc:
	//		File handler abstraction;
	//
	//---------------------------------------------------------------------------

	class CFileDescriptor
	{
		private:

			// file descriptor
			INT m_iFileDescr;

			// no copy ctor
			CFileDescriptor(const CFileDescriptor&);

		protected:

			// ctor -- accessible through inheritance only
			CFileDescriptor();

			// dtor -- accessible through inheritance only
			virtual
			~CFileDescriptor();

			// get file descriptor
			INT IFileDescr() const
			{
				return m_iFileDescr;
			}

			// open file
			void OpenInternal(const CHAR *szPath, ULONG ulMode, ULONG ulPerms);

			// close file
			void CloseInternal();

		public:

			// check if file is open
			BOOL FOpened() const
			{
				return (GPOS_FILE_DESCR_INVALID != m_iFileDescr);
			}

	};	// class CFile
}

#endif // !GPOS_CFile_H

// EOF

