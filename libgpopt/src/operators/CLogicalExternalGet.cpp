//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal, Inc.
//
//	@filename:
//		CLogicalExternalGet.cpp
//
//	@doc:
//		Implementation of external get
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CColRefTable.h"
#include "gpopt/base/COptCtxt.h"

#include "gpopt/operators/CLogicalExternalGet.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/metadata/CName.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CLogicalExternalGet::CLogicalExternalGet
//
//	@doc:
//		Ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalExternalGet::CLogicalExternalGet
	(
	IMemoryPool *pmp
	)
	:
	CLogicalGet(pmp)
{}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalExternalGet::CLogicalExternalGet
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalExternalGet::CLogicalExternalGet
	(
	IMemoryPool *pmp,
	const CName *pnameAlias,
	CTableDescriptor *ptabdesc
	)
	:
	CLogicalGet(pmp, pnameAlias, ptabdesc)
{}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalExternalGet::CLogicalExternalGet
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalExternalGet::CLogicalExternalGet
	(
	IMemoryPool *pmp,
	const CName *pnameAlias,
	CTableDescriptor *ptabdesc,
	DrgPcr *pdrgpcrOutput
	)
	:
	CLogicalGet(pmp, pnameAlias, ptabdesc, pdrgpcrOutput)
{}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalExternalGet::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CLogicalExternalGet::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}
	CLogicalExternalGet *popGet = CLogicalExternalGet::PopConvert(pop);

	return Ptabdesc() == popGet->Ptabdesc() &&
			PdrgpcrOutput()->FEqual(popGet->PdrgpcrOutput());
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalExternalGet::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalExternalGet::PopCopyWithRemappedColumns
	(
	IMemoryPool *pmp,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	DrgPcr *pdrgpcrOutput = NULL;
	if (fMustExist)
	{
		pdrgpcrOutput = CUtils::PdrgpcrRemapAndCreate(pmp, PdrgpcrOutput(), phmulcr);
	}
	else
	{
		pdrgpcrOutput = CUtils::PdrgpcrRemap(pmp, PdrgpcrOutput(), phmulcr, fMustExist);
	}
	CName *pnameAlias = GPOS_NEW(pmp) CName(pmp, Name());

	CTableDescriptor *ptabdesc = Ptabdesc();
	ptabdesc->AddRef();

	return GPOS_NEW(pmp) CLogicalExternalGet(pmp, pnameAlias, ptabdesc, pdrgpcrOutput);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalExternalGet::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalExternalGet::PxfsCandidates
	(
	IMemoryPool *pmp
	)
	const
{
	CXformSet *pxfs = GPOS_NEW(pmp) CXformSet(pmp);
	(void) pxfs->FExchangeSet(CXform::ExfExternalGet2ExternalScan);

	return pxfs;
}

// EOF

