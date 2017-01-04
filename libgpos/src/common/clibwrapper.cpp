//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (c) 2004-2015 Pivotal Software, Inc.
//
//	@filename:
//		clibwrapper.cpp
//
//	@doc:
//		Wrapper for functions in C library
//
//---------------------------------------------------------------------------

#include <cxxabi.h>
#include <dlfcn.h>
#include <errno.h>
#include <fenv.h>
#include <math.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <time.h>
#include <unistd.h>
#include <wchar.h>

#include "gpos/base.h"
#include "gpos/assert.h"
#include "gpos/utils.h"

#include "gpos/common/clibwrapper.h"
#include "gpos/error/CException.h"
#include "gpos/task/IWorker.h"

using namespace gpos;


#ifdef GPOS_sparc

#include <ucontext.h>

//---------------------------------------------------------------------------
//	@function:
//		clib::IGetContext
//
//	@doc:
//		Get current user context
//
//---------------------------------------------------------------------------
INT
gpos::clib::IGetContext
	(
	ucontext_t *puc
	)
{
	INT iRes = getcontext(puc);

	GPOS_ASSERT_(0 == iRes && "Failed to retrieve stack context");

	return iRes;
}


//---------------------------------------------------------------------------
//	@function:
//		clib::IWalkContext
//
//	@doc:
//		Call the user-supplied function for each routine found on
//		the call stack and each signal handler invoked
//
//---------------------------------------------------------------------------
INT
gpos::clib::IWalkContext
	(
	const ucontext_t *puc,
	PFnCallback fnCallback,
	void *pvArg
	)
{
	INT iRes = walkcontext(puc, fnCallback, pvArg);

	GPOS_ASSERT_(0 == iRes && "Failed to walk stack context");

	return iRes;
}

#endif //GPOS_sparc



//---------------------------------------------------------------------------
//	@function:
//		clib::USleep
//
//	@doc:
//		Sleep given number of microseconds
//
//---------------------------------------------------------------------------
void
gpos::clib::USleep
	(
	ULONG ulUs
	)
{
	GPOS_ASSERT(1000000 >= ulUs);

	// ignore return value
	(void) usleep(ulUs);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::IStrCmp
//
//	@doc:
//		Compare two strings
//
//---------------------------------------------------------------------------
INT
gpos::clib::IStrCmp
	(
	const CHAR *szLhs,
	const CHAR *szRhs
	)
{
	GPOS_ASSERT(NULL != szLhs);
	GPOS_ASSERT(NULL != szRhs);

	return strcmp(szLhs, szRhs);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::IStrNCmp
//
//	@doc:
//		Compare two strings up to a specified number of characters
//
//---------------------------------------------------------------------------
INT
gpos::clib::IStrNCmp
	(
	const CHAR *szLhs,
	const CHAR *szRhs,
	SIZE_T ulNum
	)
{
	GPOS_ASSERT(NULL != szLhs);
	GPOS_ASSERT(NULL != szRhs);

	return strncmp(szLhs, szRhs, ulNum);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::IMemCmp
//
//	@doc:
//		Compare a specified number of bytes of two regions of memory
//---------------------------------------------------------------------------
INT
gpos::clib::IMemCmp
	(
	const void* pvLhs,
	const void* pvRhs,
	SIZE_T ulNum
	)
{
	GPOS_ASSERT(NULL != pvLhs);
	GPOS_ASSERT(NULL != pvRhs);

	return memcmp(pvLhs, pvRhs, ulNum);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::IWcsNCmp
//
//	@doc:
//		Compare two strings up to a specified number of wide characters
//
//---------------------------------------------------------------------------
INT
gpos::clib::IWcsNCmp
	(
	const WCHAR *wszLhs,
	const WCHAR *wszRhs,
	SIZE_T ulNum
	)
{
	GPOS_ASSERT(NULL != wszLhs);
	GPOS_ASSERT(NULL != wszRhs);

	return wcsncmp(wszLhs, wszRhs, ulNum);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::WszWcsNCpy
//
//	@doc:
//		Copy two strings up to a specified number of wide characters
//
//---------------------------------------------------------------------------
WCHAR*
gpos::clib::WszWcsNCpy
	(
	WCHAR *wszDest,
	const WCHAR *wszSrc,
	SIZE_T ulNum
	)
{
	GPOS_ASSERT(NULL != wszDest);
	GPOS_ASSERT(NULL != wszSrc && ulNum > 0);

	// check for overlap
	GPOS_ASSERT(((wszSrc + ulNum) <= wszDest) || ((wszDest + ulNum) <= wszSrc));

	return wcsncpy(wszDest, wszSrc, ulNum);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::PvMemCpy
//
//	@doc:
//		Copy a specified number of bytes between two memory areas
//
//---------------------------------------------------------------------------
void*
gpos::clib::PvMemCpy
	(
	void *pvDest,
	const void* pvSrc,
	SIZE_T ulNum
	)
{
	GPOS_ASSERT(NULL != pvDest);

	GPOS_ASSERT(NULL != pvSrc && ulNum > 0);

#ifdef GPOS_DEBUG
	const BYTE* szSrcAddr = static_cast<const BYTE*>(pvSrc);
	const BYTE* szDestAddr = static_cast<const BYTE*>(pvDest);
#endif // GPOS_DEBUG

	// check for overlap
	GPOS_ASSERT(((szSrcAddr + ulNum) <= szDestAddr) || ((szDestAddr + ulNum) <= szSrcAddr));

	return memcpy(pvDest, pvSrc, ulNum);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::WszWMemCpy
//
//	@doc:
//		Copy a specified number of wide characters
//
//---------------------------------------------------------------------------
WCHAR*
gpos::clib::WszWMemCpy
	(
	WCHAR *wszDest,
	const WCHAR *wszSrc,
	SIZE_T ulNum
	)
{
	GPOS_ASSERT(NULL != wszDest);
	GPOS_ASSERT(NULL != wszSrc && ulNum > 0);

#ifdef GPOS_DEBUG
	const WCHAR* wszSrcAddr = static_cast<const WCHAR*>(wszSrc);
	const WCHAR* wszDestAddr = static_cast<WCHAR*>(wszDest);
#endif

	// check for overlap
	GPOS_ASSERT(((wszSrcAddr + ulNum) <= wszDestAddr) || ((wszDestAddr + ulNum) <= wszSrcAddr));

	return wmemcpy(wszDest, wszSrc, ulNum);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::SzStrNCpy
//
//	@doc:
//		Copy a specified number of characters
//
//---------------------------------------------------------------------------
CHAR*
gpos::clib::SzStrNCpy
	(
	CHAR *szDest,
	const CHAR *szSrc,
	SIZE_T ulNum
	)
{
	GPOS_ASSERT(NULL != szDest);
	GPOS_ASSERT(NULL != szSrc && ulNum > 0);
	GPOS_ASSERT(((szSrc + ulNum) <= szDest) || ((szDest + ulNum) <= szSrc));

	return strncpy(szDest, szSrc, ulNum);
}

//---------------------------------------------------------------------------
//	@function:
//		clib::SzStrChr
//
//	@doc:
//		Find the first occurrence of the character c (converted to a char) in
//		the null-terminated string beginning at szSrc. Returns a pointer to the
//		located character, or a null pointer if no match was found
//
//---------------------------------------------------------------------------
CHAR *
gpos::clib::SzStrChr
	(
	const CHAR *szSrc,
	INT c
	)
{
	GPOS_ASSERT(NULL != szSrc);

	return (CHAR *) strchr (szSrc, c);
}

//---------------------------------------------------------------------------
//	@function:
//		clib::PvMemSet
//
//	@doc:
//		Set the bytes of a given memory block to a specific value
//
//---------------------------------------------------------------------------
void*
gpos::clib::PvMemSet
	(
	void *pvDest,
	INT iValue,
	SIZE_T ulNum
	)
{
	GPOS_ASSERT(NULL != pvDest);
	GPOS_ASSERT_IFF(0 <= iValue, 255 >= iValue);

	return memset(pvDest, iValue, ulNum);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::QSort
//
//	@doc:
//		Sort a specified number of elements
//
//---------------------------------------------------------------------------
void
gpos::clib::QSort
	(
	void *pvDest,
	SIZE_T ulNum,
	SIZE_T ulSize,
	PFnComparator fnComparator
	)
{
	GPOS_ASSERT(NULL != pvDest);

	qsort(pvDest, ulNum, ulSize, fnComparator);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::IGetOpt
//
//	@doc:
//		Parse the command-line arguments
//
//---------------------------------------------------------------------------
INT
gpos::clib::IGetOpt
	(
	INT iArgc,
	CHAR * const rgszArgv[],
	const CHAR *szOptstring
	)
{
	return getopt(iArgc, rgszArgv, szOptstring);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::LStrToL
//
//	@doc:
//		Convert string to long integer
//
//---------------------------------------------------------------------------
LINT
gpos::clib::LStrToL
	(
	const CHAR *szVal,
	CHAR **ppcEnd,
	ULONG ulBase
	)
{
	GPOS_ASSERT(NULL != szVal);
	GPOS_ASSERT(0 == ulBase || 2 == ulBase || 10 == ulBase || 16 == ulBase);

	return strtol(szVal, ppcEnd, ulBase);
}

//---------------------------------------------------------------------------
//	@function:
//		clib::LStrToLL
//
//	@doc:
//		Convert string to long long integer
//
//---------------------------------------------------------------------------
LINT
gpos::clib::LStrToLL
	(
	const CHAR *szVal,
	CHAR **ppcEnd,
	ULONG ulBase
	)
{
	GPOS_ASSERT(NULL != szVal);
	GPOS_ASSERT(0 == ulBase || 2 == ulBase || 10 == ulBase || 16 == ulBase);

	return strtoll(szVal, ppcEnd, ulBase);
}

//---------------------------------------------------------------------------
//	@function:
//		clib::UlRandR
//
//	@doc:
//		Return a pseudo-random integer between 0 and RAND_MAX
//
//---------------------------------------------------------------------------
ULONG
gpos::clib::UlRandR
	(
	ULONG *pulSeed
	)
{
	GPOS_ASSERT(NULL != pulSeed);

	INT iRes = rand_r(pulSeed);

	GPOS_ASSERT(iRes >= 0 && iRes <= RAND_MAX);

	return static_cast<ULONG>(iRes);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::IVswPrintf
//
//	@doc:
//		Format wide character output conversion
//
//---------------------------------------------------------------------------
INT
gpos::clib::IVswPrintf
	(
	WCHAR *wszStr,
	SIZE_T ulMaxLen,
	const WCHAR * wszFormat,
	VA_LIST vaArgs
	)
{
	GPOS_ASSERT(NULL != wszStr);
	GPOS_ASSERT(NULL != wszFormat);

	return vswprintf(wszStr, ulMaxLen, wszFormat, vaArgs);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::IVsnPrintf
//
//	@doc:
//		Format string
//
//---------------------------------------------------------------------------
INT
gpos::clib::IVsnPrintf
	(
	CHAR *szSrc,
	SIZE_T ulSize,
	const CHAR *szFormat,
	VA_LIST vaArgs
	)
{
	GPOS_ASSERT(NULL != szSrc);
	GPOS_ASSERT(NULL != szFormat);

	return vsnprintf(szSrc, ulSize, szFormat, vaArgs);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::StrErrorR
//
//	@doc:
//		Return string describing error number
//
//---------------------------------------------------------------------------
void
gpos::clib::StrErrorR
	(
	INT iErrnum,
	CHAR *szbuf,
	SIZE_T ulBufLen
	)
{
	GPOS_ASSERT(NULL != szbuf);

#ifdef _GNU_SOURCE
	// GNU-specific strerror_r() returns char*.
	CHAR *szError = strerror_r(iErrnum, szbuf, ulBufLen);
	GPOS_ASSERT(NULL != szError);

	// GNU strerror_r() may return a pointer to a static error string.
	// Copy it into 'szbuf' if that is the case.
	if (szError != szbuf) {
		strncpy(szbuf, szError, ulBufLen);
		// Ensure null-terminated.
		szbuf[ulBufLen - 1] = '\0';
	}
#else  // !_GNU_SOURCE
	// POSIX.1-2001 standard strerror_r() returns int.
#ifdef GPOS_DEBUG
	INT iStrErrorCode =
#endif
			strerror_r(iErrnum, szbuf, ulBufLen);
	GPOS_ASSERT(0 == iStrErrorCode);

#endif
}


//---------------------------------------------------------------------------
//	@function:
//		clib::UlWcsLen
//
//	@doc:
//		Calculate the length of a wide-character string
//
//---------------------------------------------------------------------------
ULONG
gpos::clib::UlWcsLen
	(
	const WCHAR *wszDest
	)
{
	GPOS_ASSERT(NULL != wszDest);

	return (ULONG) wcslen(wszDest);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::PtmLocalTimeR
//
//	@doc:
//		Convert the calendar time ptTime to broken-time representation;
//		Expressed relative to the user's specified time zone
//
//---------------------------------------------------------------------------
struct tm*
gpos::clib::PtmLocalTimeR
	(
	const TIME_T *ptTime,
	TIME *ptmResult
	)
{
	GPOS_ASSERT(NULL != ptTime);

	localtime_r(ptTime, ptmResult);

	GPOS_ASSERT(NULL != ptmResult);

	return ptmResult;
}


//---------------------------------------------------------------------------
//	@function:
//		clib::PvMalloc
//
//	@doc:
//		Allocate dynamic memory
//
//---------------------------------------------------------------------------
void*
gpos::clib::PvMalloc
	(
	SIZE_T ulSize
	)
{
	GPOS_ASSERT_NO_SPINLOCK;

	return malloc(ulSize);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::Free
//
//	@doc:
//		Free dynamic memory
//
//---------------------------------------------------------------------------
void
gpos::clib::Free
	(
	void *pvSrc
	)
{
	GPOS_ASSERT_NO_SPINLOCK;

	free(pvSrc);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::UlStrLen
//
//	@doc:
//		Calculate the length of a string
//
//---------------------------------------------------------------------------
ULONG
gpos::clib::UlStrLen
	(
	const CHAR *szBuf
	)
{
	GPOS_ASSERT(NULL != szBuf);

	return (ULONG) strlen(szBuf);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::IWcToMb
//
//	@doc:
//		Convert a wide character to a multibyte sequence
//
//---------------------------------------------------------------------------
INT
gpos::clib::IWcToMb
	(
	CHAR *szDest,
	WCHAR wcSrc
	)
{
	return wctomb(szDest, wcSrc);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::UlMbToWcs
//
//	@doc:
//		Convert a multibyte sequence to wide character array
//
//---------------------------------------------------------------------------
ULONG
gpos::clib::UlMbToWcs
	(
	WCHAR *wszDest,
	const CHAR *szSrc,
	SIZE_T ulLen
	)
{
	GPOS_ASSERT(NULL != wszDest);
	GPOS_ASSERT(NULL != szSrc);

	return (ULONG) mbstowcs(wszDest, szSrc, ulLen);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::WcsToMbs
//
//	@doc:
//		Convert a wide-character string to a multi-byte string
//
//---------------------------------------------------------------------------
LINT
gpos::clib::LWcsToMbs
	(
	CHAR *szDest,
	WCHAR *wszSrc,
	ULONG_PTR ulpDestSize
	)
{
	return wcstombs(szDest, wszSrc, ulpDestSize);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::DStrToD
//
//	@doc:
//		Convert string to double;
//		if conversion fails, return 0.0
//
//---------------------------------------------------------------------------
DOUBLE
gpos::clib::DStrToD
	(
	const CHAR *sz
	)
{
	return strtod(sz, NULL);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::SzGetEnv
//
//	@doc:
//		Get an environment variable
//
//---------------------------------------------------------------------------
CHAR*
gpos::clib::SzGetEnv
	(
	const CHAR *szName
	)
{
	GPOS_ASSERT(NULL != szName);

	return getenv(szName);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::SzDemangle
//
//	@doc:
//		Return a pointer to the start of the NULL-terminated
//		symbol or NULL if demangling fails
//
//---------------------------------------------------------------------------
CHAR*
gpos::clib::SzDemangle
	(
	const CHAR *szSymbol,
	CHAR *szBuffer,
	SIZE_T *pulLen,
	INT *piStatus
	)
{
	GPOS_ASSERT(NULL != szSymbol);

	CHAR* szRes = abi::__cxa_demangle(szSymbol, szBuffer, pulLen, piStatus);

	GPOS_ASSERT(-3 != *piStatus && "One of the arguments is invalid.");

	return szRes;
}


//---------------------------------------------------------------------------
//	@function:
//		clib::DlAddr
//
//	@doc:
//		Resolve symbol information from its address
//
//---------------------------------------------------------------------------
void
gpos::clib::DlAddr
	(
	void *pvAddr,
	DL_INFO *pdlInfo
	)
{
#ifdef GPOS_DEBUG
	INT iRes =
#endif
	dladdr(pvAddr, pdlInfo);

	GPOS_ASSERT(0 != iRes);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::DAbs
//
//	@doc:
//		Absolute
//
//---------------------------------------------------------------------------
DOUBLE
gpos::clib::DAbs
	(
	DOUBLE d
	)
{
	return fabs(d);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::DFloor
//
//	@doc:
//		Floor
//
//---------------------------------------------------------------------------
DOUBLE
gpos::clib::DFloor
	(
	DOUBLE d
	)
{
	return floor(d);
}

//---------------------------------------------------------------------------
//	@function:
//		clib::DLog2
//
//	@doc:
//		Log to the base 2
//
//---------------------------------------------------------------------------
DOUBLE
gpos::clib::DLog2
	(
	DOUBLE d
	)
{
	return log2(d);
}

//---------------------------------------------------------------------------
//	@function:
//		clib::DCeil
//
//	@doc:
//		Ceiling
//
//---------------------------------------------------------------------------
DOUBLE
gpos::clib::DCeil
	(
	DOUBLE d
	)
{
	return ceil(d);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::DPow
//
//	@doc:
//		Power
//
//---------------------------------------------------------------------------
DOUBLE
gpos::clib::DPow
	(
	DOUBLE dBase,
	DOUBLE dExp
	)
{
	return pow(dBase, dExp);
}


// EOF
