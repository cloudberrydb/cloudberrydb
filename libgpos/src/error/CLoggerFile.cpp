//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CLoggerFile.cpp
//
//	@doc:
//		Implementation of file logging
//---------------------------------------------------------------------------

#include "gpos/io/ioutils.h"
#include "gpos/error/CLoggerFile.h"

using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		CLoggerFile::CLoggerFile
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLoggerFile::CLoggerFile
	(
	const CHAR *szDirLoc,
	ULONG ulFiles,
	ULONG ulRotateThreshold
	)
	:
	CLogger(),
	m_ulRotateThreshold(ulRotateThreshold),
	m_ulFiles(ulFiles),
	m_strsDirLoc(m_szDirLoc, GPOS_ARRAY_SIZE(m_szDirLoc)),
	m_strsFile(m_szFile, GPOS_ARRAY_SIZE(m_szFile)),
	m_strsFileOld(m_szFileOld, GPOS_ARRAY_SIZE(m_szFileOld)),
	m_strsFileNew(m_szFileNew, GPOS_ARRAY_SIZE(m_szFileNew))
{
	GPOS_ASSERT(0 < m_ulFiles);

	GPOS_RTL_ASSERT
		(
		GPOS_SZ_LENGTH(szDirLoc) + GPOS_LOG_FILE_EXTENSION_SIZE < GPOS_ARRAY_SIZE(m_szFile) &&
		"Log directory name is too long"
		)
		;

	// permissions mask
	ULONG ulPermMask =
		S_IRUSR |  S_IWUSR |  S_IXUSR |    // user has rwx permissions
		!S_IRGRP | !S_IWGRP | !S_IXGRP |   // group has no permissions
		!S_IROTH | !S_IWOTH | !S_IXOTH     // others have no permissions
		;

	// check if path is directory and if permissions are properly set
	if (!ioutils::FDir(szDirLoc) || !ioutils::FPerms(szDirLoc, ulPermMask))
	{
		GPOS_RAISE(CException::ExmaSystem, CException::ExmiIOError, errno);
	}

	// store directory path
	m_strsDirLoc.AppendBuffer(szDirLoc);

	// create default file name
	m_strsFile.AppendBuffer(szDirLoc);
	m_strsFile.AppendBuffer("/log");

	Open();
}


//---------------------------------------------------------------------------
//	@function:
//		CLoggerFile::~CLoggerFile
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLoggerFile::~CLoggerFile()
{}


//---------------------------------------------------------------------------
//	@function:
//		CLoggerFile::Open
//
//	@doc:
//		Open log file
//
//---------------------------------------------------------------------------
void
CLoggerFile::Open()
{
	// create file with permissions '600'
	m_fw.Open(m_szFile, S_IRUSR | S_IWUSR);
}


//---------------------------------------------------------------------------
//	@function:
//		CLoggerFile::Write
//
//	@doc:
//		Write string to file
//
//---------------------------------------------------------------------------
void
CLoggerFile::Write
	(
	const WCHAR *wszLogEntry,
	ULONG // ulSev
	)
{
	// entry size in bytes
	ULONG ulEntrySize = GPOS_WSZ_LENGTH(wszLogEntry) * GPOS_SIZEOF(WCHAR) / GPOS_SIZEOF(CHAR);

	const BYTE *szLogEntry = reinterpret_cast<const BYTE*>(wszLogEntry);

	// ensure file is initialized
	if (!m_fw.FOpened())
	{
		Open();
	}

	// check if max size will be exceeded
	if (m_ulRotateThreshold < ulEntrySize + m_fw.UllSize())
	{
		Rotate();
	}

	m_fw.Write(szLogEntry, ulEntrySize);
}


//---------------------------------------------------------------------------
//	@function:
//		CLoggerFile::Rotate
//
//	@doc:
//		Rotate files
//
//---------------------------------------------------------------------------
void
CLoggerFile::Rotate()
{
	// close current log file
	m_fw.Close();

	// shift log files
	for (INT i = m_ulFiles - 2; i >= 0; i--)
	{
		BuildPath(&m_strsFileOld, i);
		BuildPath(&m_strsFileNew, i + 1);

		if (ioutils::FPathExist(m_strsFileOld.Sz()))
		{
			// rename log file if it exists
			ioutils::Move(m_strsFileOld.Sz(), m_strsFileNew.Sz());
		}
	}

	// open new log file
	Open();
}


//---------------------------------------------------------------------------
//	@function:
//		CLoggerFile::BuildPath
//
//	@doc:
//		Build path to file with given id; use id 0 for current file
//
//---------------------------------------------------------------------------
void
CLoggerFile::BuildPath
	(
	CStringStatic *pstrs,
	ULONG ulId
	)
{
	pstrs->Reset();

	if (0 < ulId)
	{
		// file: <dirPath>/log.N
		pstrs->AppendFormat("%s.%d", m_strsFile.Sz(), ulId);
	}
	else
	{
		// file: <dirPath>/log
		pstrs->AppendBuffer(m_strsFile.Sz());
	}
}

// EOF

