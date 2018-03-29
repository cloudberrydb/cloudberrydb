//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		utils.cpp
//
//	@doc:
//		Various utilities which are not necessarily gpos specific
//---------------------------------------------------------------------------
#include "gpos/types.h"
#include "gpos/utils.h"

// using 16 addresses a line fits exactly into 80 characters
#define GPOS_MEM_BPL		16


// number of stack frames to search for addresses
#define GPOS_SEARCH_STACK_FRAMES	16


using namespace gpos;

//---------------------------------------------------------------------------
//	GPOS versions of standard streams
//	Do not reference std::(w)cerr/(w)cout directly;
//---------------------------------------------------------------------------
COstreamBasic gpos::oswcerr(&std::wcerr);
COstreamBasic gpos::oswcout(&std::wcout);


//---------------------------------------------------------------------------
//	@function:
//		gpos::Print
//
//	@doc:
//		Print wide-character string
//
//---------------------------------------------------------------------------
void
gpos::Print(WCHAR *wsz)
{
	std::wcout << wsz;
}


//---------------------------------------------------------------------------
//	@function:
//		gpos::HexDump
//
//	@doc:
//		Generic memory dumper; produces regular hex dump
//
//---------------------------------------------------------------------------
IOstream&
gpos::HexDump
	(
	IOstream &os,
	const void *pv,
	ULLONG ullSize
	)
{
	for(ULONG i = 0; i < 1 + (ullSize / GPOS_MEM_BPL); i++)
	{
		// starting address of line
		BYTE *pBuf = ((BYTE*)pv) + (GPOS_MEM_BPL * i);
		os << (void*)pBuf << "  ";
		os << COstream::EsmHex;

        // individual bytes
		for(ULONG j = 0; j < GPOS_MEM_BPL; j++)
		{
			if (pBuf[j] < 16) 
			{
				os << "0";
			}

			os << (ULONG)pBuf[j] << " ";

			// separator in middle of line
			if (j + 1 == GPOS_MEM_BPL / 2)
			{
				os << "- ";
			}
		}

		// blank between hex and text dump
		os << " ";

		// text representation
		for(ULONG j = 0; j < GPOS_MEM_BPL; j++)
		{
			// print only 'visible' characters
			if(pBuf[j] >= 0x20 && pBuf[j] <= 0x7f)
			{
				// cast to CHAR to avoid stream from (mis-)interpreting BYTE
				os << (CHAR)pBuf[j];
			}
			else
			{
				os << ".";
			}
		}
		os << COstream::EsmDec << std::endl;
	}
	return os;
}


//---------------------------------------------------------------------------
//	@function:
//		gpos::UlHashByteArray
//
//	@doc:
//		Generic hash function for an array of BYTEs
//		Taken from D. E. Knuth;
//
//---------------------------------------------------------------------------
ULONG
gpos::UlHashByteArray
	( 
	const BYTE *pb,
	ULONG ulSize
	)
{
	ULONG ulHash = ulSize;
		
	for(ULONG i = 0; i < ulSize; ++i)
	{
		BYTE b = pb[i];
		ulHash = ((ulHash << 5) ^ (ulHash >> 27)) ^ b;
	}
	
	return ulHash;
}


//---------------------------------------------------------------------------
//	@function:
//		gpos::UlCombineHashes
//
//	@doc:
//		Combine ULONG-based hash values
//
//---------------------------------------------------------------------------
ULONG
gpos::UlCombineHashes
	(
	ULONG ul0,
	ULONG ul1
	)
{
	ULONG rgul[2];
	rgul[0] = ul0;
	rgul[1] = ul1;

	return UlHashByteArray((BYTE*)rgul, GPOS_ARRAY_SIZE(rgul) * sizeof(rgul[0]));
}


//---------------------------------------------------------------------------
//	@function:
//		gpos::UllAdd
//
//	@doc:
//		Add two unsigned long long values, throw an exception if overflow occurs,
//
//---------------------------------------------------------------------------
ULLONG
gpos::UllAdd
	(
	ULLONG ullFst,
	ULLONG ullSnd
	)
{
	if (ullFst > gpos::ullong_max - ullSnd)
	{
		// if addition result overflows, we have (a + b > gpos::ullong_max),
		// then we need to check for  (a > gpos::ullong_max - b)
		GPOS_RAISE(CException::ExmaSystem, CException::ExmiOverflow);
	}

	ULLONG ullRes = ullFst + ullSnd;

	return ullRes;
}


//---------------------------------------------------------------------------
//	@function:
//		gpos::UllMultiply
//
//	@doc:
//		Multiply two unsigned long long values, throw an exception if overflow occurs,
//
//---------------------------------------------------------------------------
ULLONG
gpos::UllMultiply
	(
	ULLONG ullFst,
	ULLONG ullSnd
	)
{
	if (0 < ullSnd && ullFst > gpos::ullong_max / ullSnd)
	{
		// if multiplication result overflows, we have (a * b > gpos::ullong_max),
		// then we need to check for  (a > gpos::ullong_max / b)
		GPOS_RAISE(CException::ExmaSystem, CException::ExmiOverflow);

	}
	ULLONG ullRes = ullFst * ullSnd;

	return ullRes;
}

// EOF

