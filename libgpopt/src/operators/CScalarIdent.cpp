//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CScalarIdent.cpp
//
//	@doc:
//		Implementation of scalar identity operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CColRefTable.h"
#include "gpopt/operators/CScalarIdent.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CScalarIdent::UlHash
//
//	@doc:
//		Hash value built from colref and Eop
//
//---------------------------------------------------------------------------
ULONG
CScalarIdent::UlHash() const 
{
	return gpos::UlCombineHashes(COperator::UlHash(),
							   gpos::UlHashPtr<CColRef>(m_pcr));
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarIdent::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarIdent::FMatch
	(
	COperator *pop
	)
const
{
	if (pop->Eopid() == Eopid())
	{
		CScalarIdent *popIdent = CScalarIdent::PopConvert(pop);
		
		// match if column reference is same
		return Pcr() == popIdent->Pcr();
	}
	
	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarIdent::FInputOrderSensitive
//
//	@doc:
//		Not called for leaf operators
//
//---------------------------------------------------------------------------
BOOL
CScalarIdent::FInputOrderSensitive() const
{
	GPOS_ASSERT(!"Unexpected call of function FInputOrderSensitive");
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarIdent::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CScalarIdent::PopCopyWithRemappedColumns
	(
	IMemoryPool *pmp,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	ULONG ulId = m_pcr->UlId();
	CColRef *pcr = phmulcr->PtLookup(&ulId);
	if (NULL == pcr)
	{
		if (fMustExist)
		{
			// not found in hashmap, so create a new colref and add to hashmap
			CColumnFactory *pcf = COptCtxt::PoctxtFromTLS()->Pcf();

			pcr = pcf->PcrCopy(m_pcr);

#ifdef GPOS_DEBUG
			BOOL fResult =
#endif // GPOS_DEBUG
			phmulcr->FInsert(GPOS_NEW(pmp) ULONG(ulId), pcr);
			GPOS_ASSERT(fResult);
		}
		else
		{
			pcr = const_cast<CColRef *>(m_pcr);
		}
	}

	return GPOS_NEW(pmp) CScalarIdent(pmp, pcr);
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarIdent::PmdidType
//
//	@doc:
//		Expression type
//
//---------------------------------------------------------------------------
IMDId*
CScalarIdent::PmdidType() const
{
	return m_pcr->Pmdtype()->Pmdid();
}

INT
CScalarIdent::ITypeModifier() const
{
	return m_pcr->ITypeModifier();
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarIdent::FCastedScId
//
//	@doc:
// 		Is the given expression a scalar cast of a scalar identifier
//
//---------------------------------------------------------------------------
BOOL
CScalarIdent::FCastedScId
	(
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pexpr);

	// cast(col1)
	if (COperator::EopScalarCast == pexpr->Pop()->Eopid())
	{
		if (COperator::EopScalarIdent == (*pexpr)[0]->Pop()->Eopid())
		{
			return true;
		}
	}

	return false;
}

BOOL
CScalarIdent::FCastedScId
	(
	CExpression *pexpr,
	CColRef *pcr
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(NULL != pcr);

	if (!FCastedScId(pexpr))
	{
		return false;
	}

	CScalarIdent *pScIdent = CScalarIdent::PopConvert((*pexpr)[0]->Pop());

	return pcr == pScIdent->Pcr();
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarIdent::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CScalarIdent::OsPrint
	(
	IOstream &os
	)
	const
{
	os << SzId() << " ";
	m_pcr->OsPrint(os);
	
	return os;
}

// EOF

