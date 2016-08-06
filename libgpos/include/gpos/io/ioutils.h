//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		ioutils.h
//
//	@doc:
//		I/O utilities;
//		generic I/O functions that are not associated with file descriptors
//---------------------------------------------------------------------------

#ifndef GPOS_ioutils_H
#define GPOS_ioutils_H

#include <dlfcn.h>
#include <unistd.h>

#include "gpos/types.h"
#include "gpos/io/iotypes.h"

// macro for I/O error simulation
#ifdef GPOS_FPSIMULATOR
// simulate I/O error with specified address of returned error value,
// and specified errno
#define GPOS_CHECK_SIM_IO_ERR_CODE(pRes, iErrno, IOFunc) \
		do \
		{\
			if (!ioutils::FSimulateIOError(pRes, iErrno, __FILE__, __LINE__)) \
			{ \
				*pRes = IOFunc; \
			} \
		} while(0)
#else
// execute the I/O function
#define GPOS_CHECK_SIM_IO_ERR_CODE(pRes, iErrno, IOFunc) \
		do \
		{\
			GPOS_ASSERT(NULL != pRes); \
                 \
			*pRes = IOFunc; \
		} while(0)
#endif // GPOS_FPSIMULATOR

// simulate I/O error with specified address of returned error value
// and errno will set to 1 automatically
#define GPOS_CHECK_SIM_IO_ERR(pRes, IOFunc)  GPOS_CHECK_SIM_IO_ERR_CODE(pRes, 1, IOFunc)


namespace gpos
{
	namespace ioutils
	{

		// check state of file or directory
		void Stat(const CHAR *szPath, SFileStat *pfs);

		// check state of file or directory by file descriptor
		void Fstat(const INT iFd, SFileStat *pfs);

		// check if path is mapped to an accessible file or directory
		BOOL FPathExist(const CHAR *szPath);

		// get file size by file path
		ULLONG UllFileSize(const CHAR *szPath);

		// get file size by file descriptor
		ULLONG UllFileSize(const INT iFd);

		// check if path is directory
		BOOL FDir(const CHAR *szPath);

		// check if path is file
		BOOL FFile(const CHAR *szPath);

		// check permissions
		BOOL FPerms(const CHAR *szPath, ULONG ulPerms);

		// create directory with specific permissions
		void MkDir(const CHAR *szPath, ULONG ulPerms);

		// delete file
		void RmDir(const CHAR *szPath);

		// move file
		void Move(const CHAR *szOld, const CHAR *szNew);

		// delete file
		void Unlink(const CHAR *szPath);

		// open a file
		INT IOpen(const CHAR *szPath, INT iMode, INT iPerms);

		// close a file descriptor
		INT IClose(INT iFildes);

		// get file status
		INT IFStat(INT iFiledes, SFileStat *pstBuf);

		// write to a file descriptor
		INT_PTR IWrite(INT iFd, const void *pvBuf, const ULONG_PTR ulpCount);

		// read from a file descriptor
		INT_PTR IRead(INT iFd, void *pvBuf, const ULONG_PTR ulpCount);

		// create a unique temporary directory
		void SzMkDTemp(CHAR *szTemplate);

#ifdef GPOS_FPSIMULATOR
		// inject I/O error for functions whose returned value type is INT
		BOOL FSimulateIOError(INT *piRes, INT iErrno, const CHAR *szFile, ULONG ulLine);

#if defined(GPOS_64BIT) || defined(GPOS_Darwin)
		// inject I/O error for functions whose returned value type is INT_PTR
		inline
		BOOL FSimulateIOError(INT_PTR *piRes, INT iErrno, const CHAR *szFile, ULONG ulLine)
		{
			return FSimulateIOError((INT*) piRes, iErrno, szFile, ulLine);
		}
#endif

		// inject I/O error for functions whose returned value type is CHAR*
		BOOL FSimulateIOError(CHAR **ppfRes, INT iErrno, const CHAR *szFile, ULONG ulLine);
#endif // GPOS_FPSIMULATOR

	}	// namespace ioutils
}

#endif // !GPOS_ioutils_H

// EOF

