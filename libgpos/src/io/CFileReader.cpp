//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CFileReader.cpp
//
//	@doc:
//		Implementation of file handler for raw input
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/io/ioutils.h"
#include "gpos/io/CFileReader.h"

using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		CFileReader::CFileReader
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CFileReader::CFileReader()
	:
	CFileDescriptor(),
	m_ullSize(0),
	m_ullReadSize(0)
{}


//---------------------------------------------------------------------------
//	@function:
//		CFileReader::~CFileReader
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CFileReader::~CFileReader()
{}


//---------------------------------------------------------------------------
//	@function:
//		CFileReader::Open
//
//	@doc:
//		Open file for reading
//
//---------------------------------------------------------------------------
void
CFileReader::Open
	(
	const CHAR *szPath,
	const ULONG ulPerms
	)
{
	GPOS_ASSERT(NULL != szPath);

	OpenInternal(szPath, O_RDONLY, ulPerms);

	m_ullSize = ioutils::UllFileSize(szPath);
}


//---------------------------------------------------------------------------
//	@function:
//		CFileReader::Close
//
//	@doc:
//		Close file after reading
//
//---------------------------------------------------------------------------
void
CFileReader::Close()
{
	CloseInternal();
	m_ullSize = 0;
}


//---------------------------------------------------------------------------
//	@function:
//		CFileReader::Read
//
//	@doc:
//		Read bytes from file
//
//---------------------------------------------------------------------------
ULONG_PTR
CFileReader::UlpRead
	(
	BYTE *pb,
	const ULONG_PTR ulpReadSize
	)
{
	GPOS_ASSERT(CFileDescriptor::FOpened() && "Attempt to read from invalid file descriptor");
	GPOS_ASSERT(0 < ulpReadSize);
	GPOS_ASSERT(NULL != pb);

	ULONG_PTR ulpBytesLeft = ulpReadSize;

	while (0 < ulpBytesLeft)
	{
			INT_PTR iBytes = -1;

    	 	// read from file and check to simulate I/O error
    	 	GPOS_CHECK_SIM_IO_ERR(&iBytes, ioutils::IRead(IFileDescr(), pb, ulpBytesLeft));

    	 	// reach the end of file
    	 	if (0 == iBytes)
    	 	{
    	 		break;
    	 	}

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

    	 	ulpBytesLeft -= iBytes;
    	 	pb += iBytes;
    	 	m_ullReadSize += iBytes;
	};

    return ulpReadSize - ulpBytesLeft;
}


//---------------------------------------------------------------------------
//	@function:
//		CFileReader::UllSize
//
//	@doc:
//		Get file size
//
//---------------------------------------------------------------------------
ULLONG
CFileReader::UllSize() const
{
	return m_ullSize;
}


//---------------------------------------------------------------------------
//	@function:
//		CFileReader::UllReadSize
//
//	@doc:
//		Get file read size
//
//---------------------------------------------------------------------------
ULLONG
CFileReader::UllReadSize() const
{
	return m_ullReadSize;
}

// EOF

