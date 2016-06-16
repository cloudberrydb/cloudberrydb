//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CLoggerFile.h
//
//	@doc:
//		Implementation of logging interface over file
//---------------------------------------------------------------------------
#ifndef GPOS_CLoggerFile_H
#define GPOS_CLoggerFile_H

#include "gpos/error/CLogger.h"
#include "gpos/io/CFileWriter.h"
#include "gpos/string/CStringStatic.h"

#define GPOS_LOG_FILE_EXTENSION_SIZE   (16)

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CLoggerFile
	//
	//	@doc:
	//		File logging;
	//		implements file rotation;
	//		log files are named as log (current). log.1, log.2, .. log.N;
	//		rotation takes place when max file size is reached;
	//
	//---------------------------------------------------------------------------

	class CLoggerFile : public CLogger
	{
		private:

			// file writer
			CFileWriter m_fw;

			// rotate threshold
			ULONG m_ulRotateThreshold;

			// number of rotation files
			ULONG m_ulFiles;

			// directory location (absolute path)
			CHAR m_szDirLoc[GPOS_FILE_NAME_BUF_SIZE];

			// buffer for file name (absolute path)
			CHAR m_szFile[GPOS_FILE_NAME_BUF_SIZE];

			// buffer for old file name (absolute path)
			CHAR m_szFileOld[GPOS_FILE_NAME_BUF_SIZE];

			// buffer for new file name (absolute path)
			CHAR m_szFileNew[GPOS_FILE_NAME_BUF_SIZE];

			// wrapper around buffer for directory location
			CStringStatic m_strsDirLoc;

			// wrapper around buffer for file name
			CStringStatic m_strsFile;

			// wrapper around buffer for old file name
			CStringStatic m_strsFileOld;

			// wrapper around buffer for new file name
			CStringStatic m_strsFileNew;

			// open log file
			void Open();

			// rotate files
			void Rotate();

			// build path to file with given id; use id 0 for current file
			void BuildPath(CStringStatic *pstrs, ULONG ulId);

			// no copy ctor
			CLoggerFile(const CLoggerFile&);

			// write string to file
			void Write(const WCHAR *wszLogEntry, ULONG ulSev);

		public:

			// ctor
			CLoggerFile(const CHAR *szDirLoc, ULONG ulFiles, ULONG ulRotateThreshold);

			// dtor
			virtual
			~CLoggerFile();

	};	// class CLoggerFile
}

#endif // !GPOS_CLoggerFile_H

// EOF

