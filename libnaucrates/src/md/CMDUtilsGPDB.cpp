//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2016 Pivotal Inc.
//
//	@filename:
//		CMDUtilsGPDB.cpp
//
//	@doc:
//		Implementation of utility functions that are used by GPDB related metadata
//
//---------------------------------------------------------------------------

#include "naucrates/md/CMDUtilsGPDB.h"

using namespace gpdxl;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CMDUtilsGPDB::InitializeMDColInfo
//
//	@doc:
//		From the array of metadata columns populate the following output variables
//		phmiulAttno2Pos - attribute number to column array position mapping
//		pdrgpulNonDroppedCols - array of non-dropped columns
//		phmululNonDroppedCols - column array position mapping to position array of non-dropped columns
//---------------------------------------------------------------------------
void
CMDUtilsGPDB::InitializeMDColInfo
	(
	IMemoryPool *pmp,
	DrgPmdcol *pdrgpmdcol,
	HMIUl *phmiulAttno2Pos,
	DrgPul *pdrgpulNonDroppedCols,
	HMUlUl *phmululNonDroppedCols
	)
{
	GPOS_ASSERT(NULL != pdrgpmdcol);
	GPOS_ASSERT(NULL != phmiulAttno2Pos);
	GPOS_ASSERT(NULL != pdrgpulNonDroppedCols);

	const ULONG ulArity = pdrgpmdcol->UlLength();
	ULONG ulPosNonDropped = 0;
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		const IMDColumn *pmdcol = (*pdrgpmdcol)[ul];

		(void) phmiulAttno2Pos->FInsert
								(
								GPOS_NEW(pmp) INT(pmdcol->IAttno()),
								GPOS_NEW(pmp) ULONG(ul)
								);


		if (!pmdcol->FDropped())
		{
			pdrgpulNonDroppedCols->Append(GPOS_NEW(pmp) ULONG(ul));
			if (NULL != phmululNonDroppedCols)
			{
				(void) phmululNonDroppedCols->FInsert(GPOS_NEW(pmp) ULONG(ul), GPOS_NEW(pmp) ULONG(ulPosNonDropped));
			}
			ulPosNonDropped++;
		}
	}
}

// EOF
