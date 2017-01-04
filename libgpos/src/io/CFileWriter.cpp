//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CFileWriter.cpp
//
//	@doc:
//		Implementation of file handler for raw output
//---------------------------------------------------------------------------

#include <fcntl.h>

#include "gpos/base.h"
#include "gpos/io/ioutils.h"
#include "gpos/io/CFileWriter.h"

using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		CFileWriter::CFileWriter
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CFileWriter::CFileWriter()
	:
	CFileDescriptor(),
	m_ullSize(0)
{}


//---------------------------------------------------------------------------
//	@function:
//		CFileWriter::Open
//
//	@doc:
//		Open file for writing
//
//---------------------------------------------------------------------------
void
CFileWriter::Open
	(
	const CHAR *szPath,
	ULONG ulPerms
	)
{
	GPOS_ASSERT(NULL != szPath);

	OpenInternal(szPath, O_CREAT | O_WRONLY | O_RDONLY | O_TRUNC, ulPerms);

	GPOS_ASSERT(0 == ioutils::UllFileSize(szPath));

	m_ullSize = 0;
}


//---------------------------------------------------------------------------
//	@function:
//		CFileWriter::Close
//
//	@doc:
//		Close file after writing
//
//---------------------------------------------------------------------------
void
CFileWriter::Close()
{
	CloseInternal();

	m_ullSize = 0;
}


//---------------------------------------------------------------------------
//	@function:
//		CFileWriter::Write
//
//	@doc:
//		Write bytes to file
//
//---------------------------------------------------------------------------
void
CFileWriter::Write
	(
	const BYTE *pb,
	const ULONG_PTR ulpWriteSize
	)
{
	GPOS_ASSERT(CFileDescriptor::FOpened() && "Attempt to write to invalid file descriptor");
	GPOS_ASSERT(0 < ulpWriteSize);
	GPOS_ASSERT(NULL != pb);

	ULONG_PTR ulpBytesLeft = ulpWriteSize;

	while (0 < ulpBytesLeft)
	{
		INT_PTR iBytes = -1;

		// write to file and check to simulate I/O error
		GPOS_CHECK_SIM_IO_ERR(&iBytes, ioutils::IWrite(IFileDescr(), pb, ulpBytesLeft));

		// check for error
		if (-1 == iBytes)
		{
			// in case an interrupt was received we retry
			if (EINTR == errno)
			{
				GPOS_CHECK_ABORT;

				continue;
			}

			GPOS_RAISE(CException::ExmaSystem, CException::ExmiIOError, errno);
		}

		GPOS_ASSERT(iBytes <= (INT_PTR) ulpBytesLeft);

		// increase file size
		m_ullSize += iBytes;
		pb += iBytes;
		ulpBytesLeft -= iBytes;
	}
}

// EOF

