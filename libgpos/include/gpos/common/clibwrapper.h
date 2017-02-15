//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (c) 2004-2015 Pivotal Software, Inc.
//
//	@filename:
//	       	clibwrapper.h
//
//	@doc:
//	       	Wrapper for functions in C library
//
//---------------------------------------------------------------------------

#ifndef GPOS_clibwrapper_H
#define GPOS_clibwrapper_H

#define VA_START(vaList, last)  va_start(vaList, last);
#define VA_END(vaList)		va_end(vaList)
#define VA_ARG(vaList, type)    va_arg(vaList,type)

#include <unistd.h>
#include "gpos/types.h"
#include "gpos/common/clibtypes.h"

namespace gpos
{
	namespace clib
	{

		typedef INT (*PFnComparator)(const void *, const void *);

#ifdef GPOS_sparc

#include <ucontext.h>

		typedef INT (*PFnCallback)(ULONG_PTR, INT, void *);

		// get current user context
		INT IGetContext(ucontext_t *puc);

		// call the user-supplied function fnCallback for each routine found on
		// the call stack and each signal handler invoked
		INT IWalkContext(const ucontext_t *puc, PFnCallback fnCallback, void *pvArg);

#endif

		// get an environment variable
		CHAR *SzGetEnv(const CHAR *szName);

		// compare a specified number of bytes of two regions of memory
		INT IMemCmp(const void *pvLhs, const void *pvRhs, SIZE_T ulNum);

		 // sleep given number of microseconds
		void USleep(ULONG ulUs);

		// compare two strings
		INT IStrCmp(const CHAR *szLhs, const CHAR *szRhs);

		// compare two strings up to a specified number of characters
		INT IStrNCmp(const CHAR *szLhs, const CHAR *szRhs, SIZE_T ulNum);

		// compare two strings up to a specified number of wide characters
		INT IWcsNCmp(const WCHAR *wszLhs, const WCHAR *wszRhs, SIZE_T ulNum);

		// copy two strings up to a specified number of wide characters
		WCHAR *WszWcsNCpy(WCHAR *wszDest, const WCHAR *wszSrc, SIZE_T ulNum);

		// copy a specified number of bytes between two memory areas
		void* PvMemCpy(void *pvDest, const void* pvSrc, SIZE_T ulNum);

		// copy a specified number of wide characters
		WCHAR *WszWMemCpy(WCHAR *wszDest, const WCHAR *wszSrc, SIZE_T ulNum);

		// copy a specified number of characters
		CHAR *SzStrNCpy(CHAR *szDest, const CHAR *szSrc, SIZE_T ulNum);

		// find the first occurrence of the character c in szSrc
		CHAR *SzStrChr(const CHAR *szSrc, INT c);

		// set a specified number of bytes to a specified value
		void* PvMemSet(void *pvDest, INT iValue, SIZE_T ulNum);

		// calculate the length of a wide-character string
		ULONG UlWcsLen(const WCHAR *wszDest);

		// calculate the length of a string
		ULONG UlStrLen(const CHAR *szBuf);

		// sort a specified number of elements
		void QSort(void *pvDest, SIZE_T ulNum, SIZE_T ulSize,  PFnComparator fnComparator);

		// parse command-line options
		INT IGetOpt(INT iArgc, CHAR * const rgszArgv[], const CHAR *szOptstring);

		// convert string to long integer
		LINT LStrToL(const CHAR *szVal, CHAR **ppcEnd, ULONG ulBase);

		// convert string to long long integer
		LINT LStrToLL(const CHAR *szVal, CHAR **ppcEnd, ULONG ulBase);

		// convert string to double
		DOUBLE DStrToD(const CHAR *sz);

		// return a pseudo-random integer between 0 and RAND_MAX
		ULONG UlRandR(ULONG *pulSeed);

		// format wide character output conversion
		INT IVswPrintf(WCHAR *wszStr, SIZE_T ulMaxLen, const WCHAR * wszFormat, VA_LIST vaArgs);

		// format string
		INT IVsnPrintf(CHAR *szSrc, SIZE_T ulSize, const CHAR *szFormat, VA_LIST vaArgs);

		// return string describing error number
		void StrErrorR(INT iErrnum, CHAR *szbuf, SIZE_T ulBufLen);

		// convert the calendar time ptTime to broken-time representation
		TIME *PtmLocalTimeR(const TIME_T *ptTime, TIME *ptmResult);

		// allocate dynamic memory
		void *PvMalloc(SIZE_T ulSize);

		// free dynamic memory
		void Free(void *pvSrc);

		// convert a wide character to a multibyte sequence
		INT IWcToMb(CHAR *szDest, WCHAR wcSrc);

		// convert a wide-character string to a multi-byte string
		LINT LWcsToMbs(CHAR *szDest, WCHAR *wszSrc, ULONG_PTR ulpDestSize);

		// convert a multibyte sequence to wide character array
		ULONG UlMbToWcs(WCHAR *wszDest, const CHAR *szSrc, SIZE_T ulLen);

		// return a pointer to the start of the NULL-terminated symbol
		CHAR *SzDemangle(const CHAR *szSymbol, CHAR *szBuffer, SIZE_T *pulLen, INT *piStatus);

		// resolve symbol information from its address
		void DlAddr(void *pvAddr, DL_INFO *pdlInfo);

	} //namespace clib
}

#endif // !GPOS_clibwrapper_H

// EOF
