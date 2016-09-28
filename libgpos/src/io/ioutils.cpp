//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		ioutils.cpp
//
//	@doc:
//		Implementation of I/O utilities
//---------------------------------------------------------------------------

#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <stdio.h>
#include <sched.h>
#include <stdlib.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/stat.h>

#include "gpos/base.h"
#include "gpos/common/clibwrapper.h"
#include "gpos/error/CFSimulator.h"
#include "gpos/error/CLogger.h"
#include "gpos/io/ioutils.h"
#include "gpos/string/CStringStatic.h"
#include "gpos/task/CAutoTraceFlag.h"
#include "gpos/task/CTaskContext.h"

using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		ioutils::Stat
//
//	@doc:
//		Check state of file or directory
//
//---------------------------------------------------------------------------
void
gpos::ioutils::Stat
	(
	const CHAR *szPath,
	SFileStat *pfs
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != szPath);
	GPOS_ASSERT(NULL != pfs);

	// reset file state
	(void) clib::PvMemSet(pfs, 0, sizeof(*pfs));

	INT iRes = -1;

	// check to simulate I/O error
	GPOS_CHECK_SIM_IO_ERR(&iRes, stat(szPath, pfs));

	if (0 != iRes)
	{
		GPOS_RAISE(CException::ExmaSystem, CException::ExmiIOError, errno);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::FStat
//
//	@doc:
//		Check state of file or directory by file descriptor
//
//---------------------------------------------------------------------------
void
gpos::ioutils::Fstat
	(
	const INT iFd,
	SFileStat *pfs
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != pfs);

	// reset file state
	(void) clib::PvMemSet(pfs, 0, sizeof(*pfs));

	INT iRes = -1;

	// check to simulate I/O error
	GPOS_CHECK_SIM_IO_ERR(&iRes, fstat(iFd, pfs));

	if (0 != iRes)
	{
		GPOS_RAISE(CException::ExmaSystem, CException::ExmiIOError, errno);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::FPathExist
//
//	@doc:
//		Check if path is mapped to an accessible file or directory
//
//---------------------------------------------------------------------------
BOOL
gpos::ioutils::FPathExist
	(
	const CHAR *szPath
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != szPath);

	SFileStat fs;

	INT iRes = stat(szPath, &fs);

	return (0 == iRes);
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::FDir
//
//	@doc:
//		Check if path is directory
//
//---------------------------------------------------------------------------
BOOL
gpos::ioutils::FDir
	(
	const CHAR *szPath
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != szPath);

	SFileStat fs;
	Stat(szPath, &fs);

	return S_ISDIR(fs.st_mode);
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::FFile
//
//	@doc:
//		Check if path is file
//
//---------------------------------------------------------------------------
BOOL
gpos::ioutils::FFile
	(
	const CHAR *szPath
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != szPath);

	SFileStat fs;
	Stat(szPath, &fs);

	return S_ISREG(fs.st_mode);
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::UllFileSize
//
//	@doc:
//		Get file size by file path
//
//---------------------------------------------------------------------------
ULLONG
gpos::ioutils::UllFileSize
	(
	const CHAR *szPath
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != szPath);
	GPOS_ASSERT(FFile(szPath));

	SFileStat fs;
	Stat(szPath, &fs);

	return fs.st_size;
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::UllFileSize
//
//	@doc:
//		Get file size by file descriptor
//
//---------------------------------------------------------------------------
ULLONG
gpos::ioutils::UllFileSize
	(
	const INT iFd
	)
{
	GPOS_ASSERT_NO_SPINLOCK;

	SFileStat fs;
	Fstat(iFd, &fs);

	return fs.st_size;
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::FPerms
//
//	@doc:
//		Check permissions
//
//---------------------------------------------------------------------------
BOOL
gpos::ioutils::FPerms
	(
	const CHAR *szPath,
	ULONG ulPerms
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != szPath);

	SFileStat fs;
	Stat(szPath, &fs);

	return (ulPerms == (fs.st_mode & ulPerms));
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::MkDir
//
//	@doc:
//		Create directory with specific permissions
//
//---------------------------------------------------------------------------
void
gpos::ioutils::MkDir
	(
	const CHAR *szPath,
	ULONG ulPerms
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != szPath);

	INT iRes = -1;

	// check to simulate I/O error
	GPOS_CHECK_SIM_IO_ERR(&iRes, mkdir(szPath, (MODE_T) ulPerms));

	if (0 != iRes)
	{
		GPOS_RAISE(CException::ExmaSystem, CException::ExmiIOError, errno);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::RmDir
//
//	@doc:
//		Delete directory
//
//---------------------------------------------------------------------------
void
gpos::ioutils::RmDir
	(
	const CHAR *szPath
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != szPath);
	GPOS_ASSERT(FDir(szPath));

	INT iRes = -1;

	// delete existing directory and check to simulate I/O error
	GPOS_CHECK_SIM_IO_ERR(&iRes, rmdir(szPath));

	if (0 != iRes)
	{
		GPOS_RAISE(CException::ExmaSystem, CException::ExmiIOError, errno);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::Move
//
//	@doc:
//		Move file from old path to new path;
//		any file currently mapped to new path is deleted
//
//---------------------------------------------------------------------------
void
gpos::ioutils::Move
	(
	const CHAR *szOld,
	const CHAR *szNew
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != szOld);
	GPOS_ASSERT(NULL != szNew);
	GPOS_ASSERT(FFile(szOld));

	// delete any existing file with the new path
	if (FPathExist(szNew))
	{
		Unlink(szNew);
	}

	INT iRes = -1;

	// rename file and check to simulate I/O error
	GPOS_CHECK_SIM_IO_ERR(&iRes, rename(szOld, szNew));

	if (0 != iRes)
	{
		GPOS_RAISE(CException::ExmaSystem, CException::ExmiIOError, errno);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::Unlink
//
//	@doc:
//		Delete file
//
//---------------------------------------------------------------------------
void
gpos::ioutils::Unlink
	(
	const CHAR *szPath
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != szPath);

	// delete existing file
	(void) unlink(szPath);
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::IOpen
//
//	@doc:
//		Open a file;
//		It shall establish the connection between a file
//		and a file descriptor
//
//---------------------------------------------------------------------------
INT
gpos::ioutils::IOpen
	(
	const CHAR *szPath,
	INT iMode,
	INT iPerms
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != szPath);

	INT iRes = open(szPath, iMode, iPerms);

	GPOS_ASSERT((0 <= iRes) || (EINVAL != errno));

	return iRes;
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::IClose
//
//	@doc:
//		Close a file descriptor
//
//---------------------------------------------------------------------------
INT
gpos::ioutils::IClose
	(
	INT iFildes
	)
{
	GPOS_ASSERT_NO_SPINLOCK;

	INT iRes = close(iFildes);

	GPOS_ASSERT(0 == iRes || EBADF != errno);

	return iRes;
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::IFStat
//
//	@doc:
//		Get file status
//
//---------------------------------------------------------------------------
INT
gpos::ioutils::IFStat
	(
	INT iFiledes,
	SFileStat *pstBuf
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != pstBuf);

	INT iRes = fstat(iFiledes, pstBuf);

	GPOS_ASSERT(0 == iRes || EBADF != errno);

	return iRes;
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::IWrite
//
//	@doc:
//		Write to a file descriptor
//
//---------------------------------------------------------------------------
INT_PTR
gpos::ioutils::IWrite
	(
	INT iFd,
	const void *pvBuf,
	const ULONG_PTR ulpCount
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != pvBuf);
	GPOS_ASSERT(0 < ulpCount);
	GPOS_ASSERT(ULONG_PTR_MAX / 2 > ulpCount);

	SSIZE_T iRes = write(iFd, pvBuf, ulpCount);

	GPOS_ASSERT((0 <= iRes) || EBADF != errno);

	return iRes;
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::IRead
//
//	@doc:
//		Read from a file descriptor
//
//---------------------------------------------------------------------------
INT_PTR
gpos::ioutils::IRead
	(
	INT iFd,
	void *pvBuf,
	const ULONG_PTR ulpCount
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != pvBuf);
	GPOS_ASSERT(0 < ulpCount);
	GPOS_ASSERT(ULONG_PTR_MAX / 2 > ulpCount);

	SSIZE_T iRes = read(iFd, pvBuf, ulpCount);

	GPOS_ASSERT((0 <= iRes) || EBADF != errno);

	return iRes;
}

//---------------------------------------------------------------------------
//	@function:
//		ioutils::SzMkDTemp
//
//	@doc:
//		Create a unique temporary directory
//
//---------------------------------------------------------------------------
void
gpos::ioutils::SzMkDTemp
	(
	CHAR *szTemplate
	)
{
	GPOS_ASSERT(NULL != szTemplate);

#ifdef GPOS_DEBUG
	const SIZE_T ulNumOfCmp = 6;

	SIZE_T ulSize = clib::UlStrLen(szTemplate);

	GPOS_ASSERT(ulSize > ulNumOfCmp);

	GPOS_ASSERT(0 == clib::IMemCmp("XXXXXX", szTemplate + (ulSize - ulNumOfCmp), ulNumOfCmp));
#endif	// GPOS_DEBUG

	CHAR* szRes = NULL;


#ifdef GPOS_SunOS
	// check to simulate I/O error
	GPOS_CHECK_SIM_IO_ERR(&szRes, mktemp(szTemplate));

	ioutils::MkDir(szTemplate, S_IRUSR  | S_IWUSR  | S_IXUSR);
#else
	// check to simulate I/O error
	GPOS_CHECK_SIM_IO_ERR(&szRes, mkdtemp(szTemplate));
#endif // GPOS_SunOS

	if (NULL == szRes)
	{
		GPOS_RAISE(CException::ExmaSystem, CException::ExmiIOError, errno);
	}

	return;
}


#ifdef GPOS_FPSIMULATOR


//---------------------------------------------------------------------------
//	@function:
//		FSimulateIOErrorInternal
//
//	@doc:
//		Inject I/O exception
//
//---------------------------------------------------------------------------
static BOOL
FSimulateIOErrorInternal
	(
	INT iErrno,
	const CHAR *szFile,
	ULONG ulLine
	)
{
	BOOL fRes = false;

	ITask *ptsk = ITask::PtskSelf();
	if (NULL != ptsk &&
	    ptsk->FTrace(EtraceSimulateIOError) &&
	    CFSimulator::Pfsim()->FNewStack(CException::ExmaSystem, CException::ExmiIOError) &&
	    !GPOS_MATCH_EX(ptsk->Perrctxt()->Exc(), CException::ExmaSystem, CException::ExmiIOError))
	{
		// disable simulation temporarily to log injection
		CAutoTraceFlag(EtraceSimulateIOError, false);

		CLogger *plogger = dynamic_cast<CLogger*>(ITask::PtskSelf()->Ptskctxt()->PlogErr());
		if (!plogger->FLogging())
		{
			GPOS_TRACE_FORMAT_ERR("Simulating I/O error at %s:%d", szFile, ulLine);
		}

		errno = iErrno;

		if (ptsk->Perrctxt()->FPending())
		{
			ptsk->Perrctxt()->Reset();
		}

		// inject I/O error
		fRes = true;
	}

	return fRes;
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::FSimulateIOError
//
//	@doc:
//		Inject I/O exception for functions
//		whose returned value type is INT
//
//---------------------------------------------------------------------------
BOOL
gpos::ioutils::FSimulateIOError
	(
	INT *piRes,
	INT iErrno,
	const CHAR *szFile,
	ULONG ulLine
	)
{
	GPOS_ASSERT(NULL != piRes);

	*piRes = -1;

	return FSimulateIOErrorInternal(iErrno, szFile, ulLine);
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::FSimulateIOError
//
//	@doc:
//		Inject I/O exception for functions
//		whose returned value type is CHAR*
//
//---------------------------------------------------------------------------
BOOL
gpos::ioutils::FSimulateIOError
	(
	CHAR **pszRes,
	INT iErrno,
	const CHAR *szFile,
	ULONG ulLine
	)
{
	GPOS_ASSERT(NULL != pszRes);

	*pszRes = NULL;

	return FSimulateIOErrorInternal(iErrno, szFile, ulLine);
}
#endif // GPOS_FPSIMULATOR

// EOF

