//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CLogical.cpp
//
//	@doc:
//		Implementation of base class of logical operators
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "naucrates/md/IMDIndex.h"
#include "naucrates/md/IMDColumn.h"
#include "naucrates/md/IMDCheckConstraint.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CConstraintConjunction.h"
#include "gpopt/base/CConstraintInterval.h"
#include "gpopt/base/CDrvdPropRelational.h"
#include "gpopt/base/CReqdPropRelational.h"
#include "gpopt/base/CKeyCollection.h"
#include "gpopt/base/CPartIndexMap.h"
#include "gpopt/base/COptCtxt.h"

#include "gpopt/operators/CLogical.h"
#include "gpopt/operators/CLogicalApply.h"
#include "gpopt/operators/CLogicalBitmapTableGet.h"
#include "gpopt/operators/CLogicalDynamicBitmapTableGet.h"
#include "gpopt/operators/CLogicalGet.h"
#include "gpopt/operators/CLogicalDynamicGet.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/CScalarIdent.h"

#include "gpopt/optimizer/COptimizerConfig.h"

#include "naucrates/statistics/CStatistics.h"
#include "naucrates/statistics/CStatisticsUtils.h"

using namespace gpnaucrates;
using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CLogical::CLogical
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogical::CLogical
	(
	IMemoryPool *pmp
	)
	:
	COperator(pmp)
{
	GPOS_ASSERT(NULL != pmp);
	m_pcrsLocalUsed = GPOS_NEW(pmp) CColRefSet(pmp);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogical::~CLogical
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogical::~CLogical()
{
	m_pcrsLocalUsed->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CLogical::PdrgpcrCreateMapping
//
//	@doc:
//		Create output column mapping given a list of column descriptors and
//		a pointer to the operator creating that column
//
//---------------------------------------------------------------------------
DrgPcr *
CLogical::PdrgpcrCreateMapping
	(
	IMemoryPool *pmp,
	const DrgPcoldesc *pdrgpcoldesc,
	ULONG ulOpSourceId
	)
	const
{
	// get column factory from optimizer context object
	CColumnFactory *pcf = COptCtxt::PoctxtFromTLS()->Pcf();
	
	ULONG ulCols = pdrgpcoldesc->UlLength();
	
	DrgPcr *pdrgpcr = GPOS_NEW(pmp) DrgPcr(pmp, ulCols);
	for(ULONG ul = 0; ul < ulCols; ul++)
	{
		CColumnDescriptor *pcoldesc = (*pdrgpcoldesc)[ul];
		CName name(pmp, pcoldesc->Name());
		
		CColRef *pcr = pcf->PcrCreate(pcoldesc, name, ulOpSourceId);
		pdrgpcr->Append(pcr);
	}
	
	return pdrgpcr;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogical::PdrgpdrgpcrCreatePartCols
//
//	@doc:
//		Initialize array of partition columns from the array with their indexes
//
//---------------------------------------------------------------------------
DrgDrgPcr *
CLogical::PdrgpdrgpcrCreatePartCols
	(
	IMemoryPool *pmp,
	DrgPcr *pdrgpcr,
	const DrgPul *pdrgpulPart
	)
{
	GPOS_ASSERT(NULL != pdrgpcr && "Output columns cannot be NULL");
	GPOS_ASSERT(NULL != pdrgpulPart);
	
	DrgDrgPcr *pdrgpdrgpcrPart = GPOS_NEW(pmp) DrgDrgPcr(pmp);
	
	const ULONG ulPartCols = pdrgpulPart->UlLength();
	GPOS_ASSERT(0 < ulPartCols);
	
	for (ULONG ul = 0; ul < ulPartCols; ul++)
	{
		ULONG ulCol = *((*pdrgpulPart)[ul]);
		
		CColRef *pcr = (*pdrgpcr)[ulCol];
		DrgPcr * pdrgpcrCurr = GPOS_NEW(pmp) DrgPcr(pmp);
		pdrgpcrCurr->Append(pcr);
		pdrgpdrgpcrPart->Append(pdrgpcrCurr);
	}
	
	return pdrgpdrgpcrPart;
}


//		Compute an order spec based on an index

COrderSpec *
CLogical::PosFromIndex
	(
	IMemoryPool *pmp,
	const IMDIndex *pmdindex,
	DrgPcr *pdrgpcr,
	const CTableDescriptor *ptabdesc
	)
{
	//
	// compute the order spec after getting the current position of the index key
	// from the table descriptor. Index keys are relative to the
	// relation. So consider a case where we had 20 columns in a table. We
	// create an index that covers col # 20 as one of its keys. Then we drop
	// columns 10 through 15. Now the index key still points to col #20 but the
	// column ref list in pdrgpcr will only have 15 elements in it.
	//

	COrderSpec *pos = GPOS_NEW(pmp) COrderSpec(pmp);
	const ULONG ulLenKeys = pmdindex->UlKeys();

	// get relation from the metadata accessor using metadata id
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDRelation *pmdrel = pmda->Pmdrel(ptabdesc->Pmdid());

	for (ULONG  ul = 0; ul < ulLenKeys; ul++)
	{
		// This is the postion of the index key column relative to the relation
		const ULONG ulPosRel = pmdindex->UlKey(ul);

		// get the column and it's attno from the relation
		const IMDColumn *pmdcol = pmdrel->Pmdcol(ulPosRel);
		INT iAttno = pmdcol->IAttno();

		// get the position of the index key column relative to the table descriptor
		const ULONG ulPosTabDesc = ptabdesc->UlPosition(iAttno);
		CColRef *pcr = (*pdrgpcr)[ulPosTabDesc];

		IMDId *pmdid = pcr->Pmdtype()->PmdidCmp(IMDType::EcmptL);
		pmdid->AddRef();
	
		// TODO:  March 27th 2012; we hard-code NULL treatment
		// need to revisit
		pos->Append(pmdid, pcr, COrderSpec::EntLast);
	}
	
	return pos;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogical::PcrsDeriveOutputPassThru
//
//	@doc:
//		Common case of output derivation for unary operators or operators
//		that pass through the schema of only the outer child
//
//---------------------------------------------------------------------------
CColRefSet *
CLogical::PcrsDeriveOutputPassThru
	(
	CExpressionHandle &exprhdl
	)
{
	// may have additional children that are ignored, e.g., scalar children
	GPOS_ASSERT(1 <= exprhdl.UlArity());
	
	CColRefSet *pcrs = exprhdl.Pdprel(0)->PcrsOutput();
	pcrs->AddRef();
	
	return pcrs;
}
	

//---------------------------------------------------------------------------
//	@function:
//		CLogical::PcrsDeriveNotNullPassThruOuter
//
//	@doc:
//		Common case of deriving not null columns by passing through
//		not null columns from the outer child
//
//---------------------------------------------------------------------------
CColRefSet *
CLogical::PcrsDeriveNotNullPassThruOuter
	(
	CExpressionHandle &exprhdl
	)
{
	// may have additional children that are ignored, e.g., scalar children
	GPOS_ASSERT(1 <= exprhdl.UlArity());

	CColRefSet *pcrs = exprhdl.Pdprel(0)->PcrsNotNull();
	pcrs->AddRef();

	return pcrs;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogical::PcrsDeriveOutputCombineLogical
//
//	@doc:
//		Common case of output derivation by combining the schemas of all
//		children
//
//---------------------------------------------------------------------------
CColRefSet *
CLogical::PcrsDeriveOutputCombineLogical
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl
	)
{
	CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp);

	// union columns from the first N-1 children
	ULONG ulArity = exprhdl.UlArity();
	for (ULONG ul = 0; ul < ulArity - 1; ul++)
	{
		CColRefSet *pcrsChild = exprhdl.Pdprel(ul)->PcrsOutput();
		GPOS_ASSERT(pcrs->FDisjoint(pcrsChild) && "Input columns are not disjoint");

		pcrs->Union(pcrsChild);
	}

	return pcrs;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogical::PcrsDeriveNotNullCombineLogical
//
//	@doc:
//		Common case of combining not null columns from all logical
//		children
//
//---------------------------------------------------------------------------
CColRefSet *
CLogical::PcrsDeriveNotNullCombineLogical
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl
	)
{
	CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp);

	// union not nullable columns from the first N-1 children
	ULONG ulArity = exprhdl.UlArity();
	for (ULONG ul = 0; ul < ulArity - 1; ul++)
	{
		CColRefSet *pcrsChild = exprhdl.Pdprel(ul)->PcrsNotNull();
		GPOS_ASSERT(pcrs->FDisjoint(pcrsChild) && "Input columns are not disjoint");

		pcrs->Union(pcrsChild);
	}
 
	return pcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogical::PpartinfoPassThruOuter
//
//	@doc:
//		Common case of common case of passing through partition consumer array
//
//---------------------------------------------------------------------------
CPartInfo *
CLogical::PpartinfoPassThruOuter
	(
	CExpressionHandle &exprhdl
	)
{
	CPartInfo *ppartinfo = exprhdl.Pdprel(0 /*ulChildIndex*/)->Ppartinfo();
	GPOS_ASSERT(NULL != ppartinfo);
	ppartinfo->AddRef();
	return ppartinfo;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogical::PkcCombineKeys
//
//	@doc:
//		Common case of combining keys from first n - 1 children
//
//---------------------------------------------------------------------------
CKeyCollection *
CLogical::PkcCombineKeys
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl
	)
{
	CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp);
	const ULONG ulArity = exprhdl.UlArity();
	for (ULONG ul = 0; ul < ulArity - 1; ul++)
	{
		CKeyCollection *pkc = exprhdl.Pdprel(ul)->Pkc();
		if (NULL == pkc)
		{
			// if a child has no key, the operator has no key
			pcrs->Release();
			return NULL;
		}

		DrgPcr *pdrgpcr = pkc->PdrgpcrKey(pmp);
		pcrs->Include(pdrgpcr);
		pdrgpcr->Release();
	}

	return GPOS_NEW(pmp) CKeyCollection(pmp, pcrs);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogical::PkcKeysBaseTable
//
//	@doc:
//		Helper function for computing the keys in a base table
//
//---------------------------------------------------------------------------
CKeyCollection *
CLogical::PkcKeysBaseTable
	(
	IMemoryPool *pmp,
	const DrgPbs *pdrgpbsKeys,
	const DrgPcr *pdrgpcrOutput
	)
{
	const ULONG ulKeys = pdrgpbsKeys->UlLength();
	
	if (0 == ulKeys)
	{
		return NULL;
	}

	CKeyCollection *pkc = GPOS_NEW(pmp) CKeyCollection(pmp);
	
	for (ULONG ul = 0; ul < ulKeys; ul++)
	{
		CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp);

		CBitSet *pbs = (*pdrgpbsKeys)[ul];
		CBitSetIter bsiter(*pbs);
		
		while (bsiter.FAdvance())
		{
			pcrs->Include((*pdrgpcrOutput)[bsiter.UlBit()]);
		}

		pkc->Add(pcrs);
	}

	return pkc;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogical::PpartinfoDeriveCombine
//
//	@doc:
//		Common case of combining partition info of all logical children
//
//---------------------------------------------------------------------------
CPartInfo *
CLogical::PpartinfoDeriveCombine
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl
	)
{
	const ULONG ulArity = exprhdl.UlArity();
	GPOS_ASSERT(0 < ulArity);

	CPartInfo *ppartinfo = GPOS_NEW(pmp) CPartInfo(pmp);
	
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CPartInfo *ppartinfoChild = NULL;
		if (exprhdl.FScalarChild(ul))
		{
			ppartinfoChild = exprhdl.Pdpscalar(ul)->Ppartinfo();
		}
		else
		{
			ppartinfoChild = exprhdl.Pdprel(ul)->Ppartinfo();
		}
		GPOS_ASSERT(NULL != ppartinfoChild);
		CPartInfo *ppartinfoCombined = CPartInfo::PpartinfoCombine(pmp, ppartinfo, ppartinfoChild);
		ppartinfo->Release();
		ppartinfo = ppartinfoCombined;
	}

	return ppartinfo;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogical::PcrsDeriveOuter
//
//	@doc:
//		Derive outer references
//
//---------------------------------------------------------------------------
CColRefSet *
CLogical::PcrsDeriveOuter
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CColRefSet *pcrsUsedAdditional
	)
{
	ULONG ulArity = exprhdl.UlArity();
	CColRefSet *pcrsOuter = GPOS_NEW(pmp) CColRefSet(pmp);

	// collect output columns from relational children
	// and used columns from scalar children
	CColRefSet *pcrsOutput = GPOS_NEW(pmp) CColRefSet(pmp);

	CColRefSet *pcrsUsed = GPOS_NEW(pmp) CColRefSet(pmp);
	for (ULONG i = 0; i < ulArity; i++)
	{
		if (exprhdl.FScalarChild(i))
		{
			CDrvdPropScalar *pdpscalar = exprhdl.Pdpscalar(i);
			pcrsUsed->Union(pdpscalar->PcrsUsed());
		}
		else
		{
			CDrvdPropRelational *pdprel = exprhdl.Pdprel(i);
			pcrsOutput->Union(pdprel->PcrsOutput());

			// add outer references from relational children
			pcrsOuter->Union(pdprel->PcrsOuter());
		}
	}

	if (NULL != pcrsUsedAdditional)
	{
		pcrsUsed->Include(pcrsUsedAdditional);
	}

	// outer references are columns used by scalar child
	// but are not included in the output columns of relational children
	pcrsOuter->Union(pcrsUsed);
	pcrsOuter->Exclude(pcrsOutput);

	pcrsOutput->Release();
	pcrsUsed->Release();
	return pcrsOuter;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogical::PcrsDeriveOuterIndexGet
//
//	@doc:
//		Derive outer references for index get and dynamic index get operators
//
//---------------------------------------------------------------------------
CColRefSet *
CLogical::PcrsDeriveOuterIndexGet
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl
	)
{
	ULONG ulArity = exprhdl.UlArity();
	CColRefSet *pcrsOuter = GPOS_NEW(pmp) CColRefSet(pmp);

	CColRefSet *pcrsOutput = PcrsDeriveOutput(pmp, exprhdl);
	
	CColRefSet *pcrsUsed = GPOS_NEW(pmp) CColRefSet(pmp);
	for (ULONG i = 0; i < ulArity; i++)
	{
		GPOS_ASSERT(exprhdl.FScalarChild(i));
		CDrvdPropScalar *pdpscalar = exprhdl.Pdpscalar(i);
		pcrsUsed->Union(pdpscalar->PcrsUsed());
	}

	// outer references are columns used by scalar children
	// but are not included in the output columns of relational children
	pcrsOuter->Union(pcrsUsed);
	pcrsOuter->Exclude(pcrsOutput);

	pcrsOutput->Release();
	pcrsUsed->Release();
	return pcrsOuter;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogical::PcrsDeriveCorrelatedApply
//
//	@doc:
//		Derive columns from the inner child of a correlated-apply expression
//		that can be used above the apply expression
//
//---------------------------------------------------------------------------
CColRefSet *
CLogical::PcrsDeriveCorrelatedApply
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl
	)
	const
{
	GPOS_ASSERT(this == exprhdl.Pop());

	ULONG ulArity = exprhdl.UlArity();
	CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp);

	if (CUtils::FCorrelatedApply(exprhdl.Pop()))
	{
		// add inner columns of correlated-apply expression
		pcrs->Include(CLogicalApply::PopConvert(exprhdl.Pop())->PdrgPcrInner());
	}

	// combine correlated-apply columns from logical children
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		if (!exprhdl.FScalarChild(ul))
		{
			CDrvdPropRelational *pdprel = exprhdl.Pdprel(ul);
			pcrs->Union(pdprel->PcrsCorrelatedApply());
		}
	}

	return pcrs;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogical::PkcDeriveKeysPassThru
//
//	@doc:
//		Addref and return keys of n-th child
//
//---------------------------------------------------------------------------
CKeyCollection *
CLogical::PkcDeriveKeysPassThru
	(
	CExpressionHandle &exprhdl,
	ULONG ulChild
	)
{	
	CKeyCollection *pkcLeft = exprhdl.Pdprel(ulChild)->Pkc();
	
	// key collection may be NULL
	if (NULL != pkcLeft)
	{
		pkcLeft->AddRef();
	}
	
	return pkcLeft;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogical::PkcDeriveKeys
//
//	@doc:
//		Derive key collections
//
//---------------------------------------------------------------------------
CKeyCollection *
CLogical::PkcDeriveKeys
	(
	IMemoryPool *, // pmp
	CExpressionHandle & // exprhdl
	)
	const
{
	// no keys found by default
	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogical::PpcDeriveConstraintFromPredicates
//
//	@doc:
//		Derive constraint property when expression has relational children and
//		scalar children (predicates)
//
//---------------------------------------------------------------------------
CPropConstraint *
CLogical::PpcDeriveConstraintFromPredicates
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl
	)
{
	DrgPcrs *pdrgpcrs = GPOS_NEW(pmp) DrgPcrs(pmp);

	DrgPcnstr *pdrgpcnstr = GPOS_NEW(pmp) DrgPcnstr(pmp);

	// collect constraint properties from relational children
	// and predicates from scalar children
	const ULONG ulArity = exprhdl.UlArity();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		if (exprhdl.FScalarChild(ul))
		{
			CExpression *pexprScalar = exprhdl.PexprScalarChild(ul);

			// make sure it is a predicate... boolop, cmp, nulltest
			if (NULL == pexprScalar || !CUtils::FPredicate(pexprScalar))
			{
				continue;
			}

			DrgPcrs *pdrgpcrsChild = NULL;
			CConstraint *pcnstr = CConstraint::PcnstrFromScalarExpr(pmp, pexprScalar, &pdrgpcrsChild);
			if (NULL != pcnstr)
			{
				pdrgpcnstr->Append(pcnstr);

				// merge with the equivalence classes we have so far
				DrgPcrs *pdrgpcrsMerged = CUtils::PdrgpcrsMergeEquivClasses(pmp, pdrgpcrs, pdrgpcrsChild);
				pdrgpcrs->Release();
				pdrgpcrs = pdrgpcrsMerged;
			}
			CRefCount::SafeRelease(pdrgpcrsChild);
		}
		else
		{
			CDrvdPropRelational *pdprel = exprhdl.Pdprel(ul);
			CPropConstraint *ppc = pdprel->Ppc();

			// equivalence classes coming from child
			DrgPcrs *pdrgpcrsChild = ppc->PdrgpcrsEquivClasses();

			// merge with the equivalence classes we have so far
			DrgPcrs *pdrgpcrsMerged = CUtils::PdrgpcrsMergeEquivClasses(pmp, pdrgpcrs, pdrgpcrsChild);
			pdrgpcrs->Release();
			pdrgpcrs = pdrgpcrsMerged;

			// constraint coming from child
			CConstraint *pcnstr = ppc->Pcnstr();
			if (NULL != pcnstr)
			{
				pcnstr->AddRef();
				pdrgpcnstr->Append(pcnstr);
			}
		}
	}

	CConstraint *pcnstrNew = CConstraint::PcnstrConjunction(pmp, pdrgpcnstr);

	return GPOS_NEW(pmp) CPropConstraint(pmp, pdrgpcrs, pcnstrNew);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogical::PpcDeriveConstraintFromTable
//
//	@doc:
//		Derive constraint property from a table/index get
//
//---------------------------------------------------------------------------
CPropConstraint *
CLogical::PpcDeriveConstraintFromTable
	(
	IMemoryPool *pmp,
	const CTableDescriptor *ptabdesc,
	const DrgPcr *pdrgpcrOutput
	)
{
	DrgPcrs *pdrgpcrs = GPOS_NEW(pmp) DrgPcrs(pmp);

	DrgPcnstr *pdrgpcnstr = GPOS_NEW(pmp) DrgPcnstr(pmp);

	const DrgPcoldesc *pdrgpcoldesc = ptabdesc->Pdrgpcoldesc();
	const ULONG ulCols = pdrgpcoldesc->UlLength();

	DrgPcr *pdrgpcrNonSystem = GPOS_NEW(pmp) DrgPcr(pmp);

	for (ULONG ul = 0; ul < ulCols; ul++)
	{
		CColumnDescriptor *pcoldesc = (*pdrgpcoldesc)[ul];
		CColRef *pcr = (*pdrgpcrOutput)[ul];
		// we are only interested in non-system columns that are defined as
		// being NOT NULL

		if (pcoldesc->FSystemColumn())
		{
			continue;
		}

		pdrgpcrNonSystem->Append(pcr);

		if (pcoldesc->FNullable())
		{
			continue;
		}

		// add a "not null" constraint and an equivalence class
		CConstraint * pcnstr = CConstraintInterval::PciUnbounded(pmp, pcr, false /*fIncludesNull*/);

		if (pcnstr == NULL)
		{
			continue;
		}
		pdrgpcnstr->Append(pcnstr);

		CColRefSet *pcrsEquiv = GPOS_NEW(pmp) CColRefSet(pmp);
		pcrsEquiv->Include(pcr);
		pdrgpcrs->Append(pcrsEquiv);
	}

	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDRelation *pmdrel = pmda->Pmdrel(ptabdesc->Pmdid());

	const ULONG ulCheckConstraint = pmdrel->UlCheckConstraints();
	for (ULONG ul = 0; ul < ulCheckConstraint; ul++)
	{
		IMDId *pmdidCheckConstraint = pmdrel->PmdidCheckConstraint(ul);

		const IMDCheckConstraint *pmdCheckConstraint = pmda->Pmdcheckconstraint(pmdidCheckConstraint);

		// extract the check constraint expression
		CExpression *pexprCheckConstraint = pmdCheckConstraint->Pexpr(pmp, pmda, pdrgpcrNonSystem);
		GPOS_ASSERT(NULL != pexprCheckConstraint);
		GPOS_ASSERT(CUtils::FPredicate(pexprCheckConstraint));

		DrgPcrs *pdrgpcrsChild = NULL;
		CConstraint *pcnstr = CConstraint::PcnstrFromScalarExpr(pmp, pexprCheckConstraint, &pdrgpcrsChild);
		if (NULL != pcnstr)
		{
			pdrgpcnstr->Append(pcnstr);

			// merge with the equivalence classes we have so far
			DrgPcrs *pdrgpcrsMerged = CUtils::PdrgpcrsMergeEquivClasses(pmp, pdrgpcrs, pdrgpcrsChild);
			pdrgpcrs->Release();
			pdrgpcrs = pdrgpcrsMerged;
		}
		CRefCount::SafeRelease(pdrgpcrsChild);
		pexprCheckConstraint->Release();
	}

	pdrgpcrNonSystem->Release();

	return GPOS_NEW(pmp) CPropConstraint(pmp, pdrgpcrs, CConstraint::PcnstrConjunction(pmp, pdrgpcnstr));
}

//---------------------------------------------------------------------------
//	@function:
//		CLogical::PpcDeriveConstraintFromTableWithPredicates
//
//	@doc:
//		Derive constraint property from a table/index get with predicates
//
//---------------------------------------------------------------------------
CPropConstraint *
CLogical::PpcDeriveConstraintFromTableWithPredicates
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	const CTableDescriptor *ptabdesc,
	const DrgPcr *pdrgpcrOutput
	)
{
	DrgPcnstr *pdrgpcnstr = GPOS_NEW(pmp) DrgPcnstr(pmp);
	CPropConstraint *ppcTable = PpcDeriveConstraintFromTable(pmp, ptabdesc, pdrgpcrOutput);
	CConstraint *pcnstrTable = ppcTable->Pcnstr();
	if (NULL != pcnstrTable)
	{
		pcnstrTable->AddRef();
		pdrgpcnstr->Append(pcnstrTable);
	}
	DrgPcrs *pdrgpcrsEquivClassesTable = ppcTable->PdrgpcrsEquivClasses();

	CPropConstraint *ppcnstrCond = PpcDeriveConstraintFromPredicates(pmp, exprhdl);
	CConstraint *pcnstrCond = ppcnstrCond->Pcnstr();

	if (NULL != pcnstrCond)
	{
		pcnstrCond->AddRef();
		pdrgpcnstr->Append(pcnstrCond);
	}
	else if (NULL == pcnstrTable)
	{
		ppcTable->Release();
		pdrgpcnstr->Release();

		return ppcnstrCond;
	}

	DrgPcrs *pdrgpcrsCond = ppcnstrCond->PdrgpcrsEquivClasses();
	DrgPcrs *pdrgpcrs = CUtils::PdrgpcrsMergeEquivClasses(pmp, pdrgpcrsEquivClassesTable, pdrgpcrsCond);
	CPropConstraint *ppc = GPOS_NEW(pmp) CPropConstraint(pmp, pdrgpcrs, CConstraint::PcnstrConjunction(pmp, pdrgpcnstr));

	ppcnstrCond->Release();
	ppcTable->Release();

	return ppc;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogical::PpcDeriveConstraintPassThru
//
//	@doc:
//		Shorthand to addref and pass through constraint from a given child
//
//---------------------------------------------------------------------------
CPropConstraint *
CLogical::PpcDeriveConstraintPassThru
	(
	CExpressionHandle &exprhdl,
	ULONG ulChild
	)
{
	// return constraint property of child
	CPropConstraint *ppc = exprhdl.Pdprel(ulChild)->Ppc();
	if (NULL != ppc)
	{
		ppc->AddRef();
	}
	return ppc;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogical::PpcDeriveConstraintRestrict
//
//	@doc:
//		Derive constraint property only on the given columns
//
//---------------------------------------------------------------------------
CPropConstraint *
CLogical::PpcDeriveConstraintRestrict
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CColRefSet *pcrsOutput
	)
{
	// constraint property from relational child
	CPropConstraint *ppc = exprhdl.Pdprel(0)->Ppc();
	DrgPcrs *pdrgpcrs = ppc->PdrgpcrsEquivClasses();

	// construct new array of equivalence classes
	DrgPcrs *pdrgpcrsNew = GPOS_NEW(pmp) DrgPcrs(pmp);

	const ULONG ulLen = pdrgpcrs->UlLength();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		CColRefSet *pcrsEquiv = GPOS_NEW(pmp) CColRefSet(pmp);
		pcrsEquiv->Include((*pdrgpcrs)[ul]);
		pcrsEquiv->Intersection(pcrsOutput);

		if (0 < pcrsEquiv->CElements())
		{
			pdrgpcrsNew->Append(pcrsEquiv);
		}
		else
		{
			pcrsEquiv->Release();
		}
	}

	CConstraint *pcnstrChild = ppc->Pcnstr();
	if (NULL == pcnstrChild)
	{
		return GPOS_NEW(pmp) CPropConstraint(pmp, pdrgpcrsNew, NULL);
	}

	DrgPcnstr *pdrgpcnstr = GPOS_NEW(pmp) DrgPcnstr(pmp);

	// include only constraints on given columns
	CColRefSetIter crsi(*pcrsOutput);
	while (crsi.FAdvance())
	{
		CColRef *pcr = crsi.Pcr();
		CConstraint *pcnstrCol = pcnstrChild->Pcnstr(pmp, pcr);
		if (NULL == pcnstrCol)
		{
			continue;
		}

		if (pcnstrCol->FUnbounded())
		{
			pcnstrCol->Release();
			continue;
		}

		pdrgpcnstr->Append(pcnstrCol);
	}

	CConstraint *pcnstr = CConstraint::PcnstrConjunction(pmp, pdrgpcnstr);

	return GPOS_NEW(pmp) CPropConstraint(pmp, pdrgpcrsNew, pcnstr);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogical::PfpDerive
//
//	@doc:
//		Derive function properties
//
//---------------------------------------------------------------------------
CFunctionProp *
CLogical::PfpDerive
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl
	)
	const
{
	IMDFunction::EFuncStbl efs = EfsDeriveFromChildren(exprhdl, IMDFunction::EfsImmutable);

	return GPOS_NEW(pmp) CFunctionProp(efs, IMDFunction::EfdaNoSQL, exprhdl.FChildrenHaveVolatileFuncScan(), false /*fScan*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogical::PfpDeriveFromScalar
//
//	@doc:
//		Derive function properties using data access property of scalar child
//
//---------------------------------------------------------------------------
CFunctionProp *
CLogical::PfpDeriveFromScalar
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	ULONG ulScalarIndex
	)
{
	GPOS_CHECK_ABORT;
	GPOS_ASSERT(ulScalarIndex == exprhdl.UlArity() - 1);
	GPOS_ASSERT(exprhdl.FScalarChild(ulScalarIndex));

	// collect stability from all children
	IMDFunction::EFuncStbl efs = EfsDeriveFromChildren(exprhdl, IMDFunction::EfsImmutable);

	// get data access from scalar child
	CFunctionProp *pfp = exprhdl.PfpChild(ulScalarIndex);
	GPOS_ASSERT(NULL != pfp);
	IMDFunction::EFuncDataAcc efda = pfp->Efda();

	return GPOS_NEW(pmp) CFunctionProp(efs, efda, exprhdl.FChildrenHaveVolatileFuncScan(), false /*fScan*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogical::Maxcard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogical::Maxcard
	(
	IMemoryPool *, // pmp
	CExpressionHandle & // exprhdl
	)
	const
{
	// unbounded by default
	return CMaxCard();
}
	

//---------------------------------------------------------------------------
//	@function:
//		CLogical::UlJoinDepth
//
//	@doc:
//		Derive join depth
//
//---------------------------------------------------------------------------
ULONG
CLogical::UlJoinDepth
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl
	)
	const
{
	const ULONG ulArity = exprhdl.UlArity();

	// sum-up join depth of all relational children
	ULONG ulDepth = 0;
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		if (!exprhdl.FScalarChild(ul))
		{
			ulDepth = ulDepth + exprhdl.Pdprel(ul)->UlJoinDepth();
		}
	}

	return ulDepth;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogical::MaxcardDef
//
//	@doc:
//		Default max card for join and apply operators
//
//---------------------------------------------------------------------------
CMaxCard
CLogical::MaxcardDef
	(
	CExpressionHandle &exprhdl
	)
{
	const ULONG ulArity = exprhdl.UlArity();

	CMaxCard maxcard = exprhdl.Pdprel(0)->Maxcard();
	for (ULONG ul = 1; ul < ulArity - 1; ul++)
	{
		if (!exprhdl.FScalarChild(ul))
		{
			maxcard *= exprhdl.Pdprel(ul)->Maxcard();
		}
	}

	return maxcard;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogical::Maxcard
//
//	@doc:
//		Derive max card given scalar child and constraint property. If a
//		contradiction is detected then return maxcard of zero, otherwise
//		use the given default maxcard
//
//---------------------------------------------------------------------------
CMaxCard
CLogical::Maxcard
	(
	CExpressionHandle &exprhdl,
	ULONG ulScalarIndex,
	CMaxCard maxcard
	)
{
	// in case of a false condition (when the operator is not Full / Left Outer Join) or a contradiction, maxcard should be zero
	CExpression *pexprScalar = exprhdl.PexprScalarChild(ulScalarIndex);

	if (NULL != pexprScalar &&
		( (CUtils::FScalarConstFalse(pexprScalar) &&
				(COperator::EopLogicalFullOuterJoin != exprhdl.Pop()->Eopid() &&
						COperator::EopLogicalLeftOuterJoin != exprhdl.Pop()->Eopid()))
		|| CDrvdPropRelational::Pdprel(exprhdl.Pdp())->Ppc()->FContradiction()))
	{
		return CMaxCard(0 /*ull*/);
	}

	return maxcard;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogical::PcrsReqdChildStats
//
//	@doc:
//		Helper for compute required stat columns of the n-th child
//
//---------------------------------------------------------------------------
CColRefSet *
CLogical::PcrsReqdChildStats
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CColRefSet *pcrsInput,
	CColRefSet *pcrsUsed, // columns used by scalar child(ren)
	ULONG ulChildIndex
	)
{
	GPOS_ASSERT(ulChildIndex < exprhdl.UlArity() - 1);
	GPOS_CHECK_ABORT;

	CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp);
	pcrs->Union(pcrsInput);
	pcrs->Union(pcrsUsed);

	// intersect with the output columns of relational child
	pcrs->Intersection(exprhdl.Pdprel(ulChildIndex)->PcrsOutput());

	return pcrs;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogical::PcrsStatsPassThru
//
//	@doc:
//		Helper for common case of passing through required stat columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogical::PcrsStatsPassThru
	(
	CColRefSet *pcrsInput
	)
{
	GPOS_ASSERT(NULL != pcrsInput);
	GPOS_CHECK_ABORT;

	pcrsInput->AddRef();
	return pcrsInput;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogical::PstatsPassThruOuter
//
//	@doc:
//		Helper for common case of passing through derived stats
//
//---------------------------------------------------------------------------
IStatistics *
CLogical::PstatsPassThruOuter
	(
	CExpressionHandle &exprhdl
	)
{
	GPOS_CHECK_ABORT;

	IStatistics *pstats = exprhdl.Pstats(0);
	pstats->AddRef();

	return pstats;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogical::PstatsBaseTable
//
//	@doc:
//		Helper for deriving statistics on a base table
//
//---------------------------------------------------------------------------
IStatistics *
CLogical::PstatsBaseTable
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CTableDescriptor *ptabdesc,
	CColRefSet *pcrsHistExtra   // additional columns required for stats, not required by parent
	)
{
	CReqdPropRelational *prprel = CReqdPropRelational::Prprel(exprhdl.Prp());
	CColRefSet *pcrsHist = GPOS_NEW(pmp) CColRefSet(pmp);
	pcrsHist->Include(prprel->PcrsStat());
	if (NULL != pcrsHistExtra)
	{
		pcrsHist->Include(pcrsHistExtra);
	}

	CDrvdPropRelational *pdprel = exprhdl.Pdprel();
	CColRefSet *pcrsOutput = pdprel->PcrsOutput();
	CColRefSet *pcrsWidth = GPOS_NEW(pmp) CColRefSet(pmp);
	pcrsWidth->Include(pcrsOutput);
	pcrsWidth->Exclude(pcrsHist);

	const COptCtxt *poctxt = COptCtxt::PoctxtFromTLS();
	CMDAccessor *pmda = poctxt->Pmda();
	CStatisticsConfig *pstatsconf = poctxt->Poconf()->Pstatsconf();

	IStatistics *pstats = pmda->Pstats
								(
								pmp,
								ptabdesc->Pmdid(),
								pcrsHist,
								pcrsWidth,
								pstatsconf
								);

	// clean up
	pcrsWidth->Release();
	pcrsHist->Release();

	return pstats;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogical::PstatsDeriveDummy
//
//	@doc:
//		Derive dummy statistics
//
//---------------------------------------------------------------------------
IStatistics *
CLogical::PstatsDeriveDummy
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CDouble dRows
	)
	const
{
	GPOS_CHECK_ABORT;

	// return a dummy stats object that has a histogram for every
	// required-stats column
	GPOS_ASSERT(Esp(exprhdl) > EspNone);
	CReqdPropRelational *prprel = CReqdPropRelational::Prprel(exprhdl.Prp());
	CColRefSet *pcrs = prprel->PcrsStat();
	DrgPul *pdrgpulColIds = GPOS_NEW(pmp) DrgPul(pmp);
	pcrs->ExtractColIds(pmp, pdrgpulColIds);

	IStatistics *pstats = CStatistics::PstatsDummy(pmp, pdrgpulColIds, dRows);

	// clean up
	pdrgpulColIds->Release();

	return pstats;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogical::PexprPartPred
//
//	@doc:
//		Compute partition predicate to pass down to n-th child
//
//---------------------------------------------------------------------------
CExpression *
CLogical::PexprPartPred
	(
	IMemoryPool *, //pmp,
	CExpressionHandle &, //exprhdl,
	CExpression *, //pexprInput,
	ULONG //ulChildIndex
	)
	const
{
	GPOS_CHECK_ABORT;

	// the default behavior is to never pass down any partition predicates
	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogical::PdpCreate
//
//	@doc:
//		Create base container of derived properties
//
//---------------------------------------------------------------------------
CDrvdProp *
CLogical::PdpCreate
	(
	IMemoryPool *pmp
	)
	const
{
	return GPOS_NEW(pmp) CDrvdPropRelational();
}


//---------------------------------------------------------------------------
//	@function:
//		CLogical::PrpCreate
//
//	@doc:
//		Create base container of required properties
//
//---------------------------------------------------------------------------
CReqdProp *
CLogical::PrpCreate
	(
	IMemoryPool *pmp
	)
	const
{
	return GPOS_NEW(pmp) CReqdPropRelational();
}


//---------------------------------------------------------------------------
//	@function:
//		CLogical::PtabdescFromTableGet
//
//	@doc:
//		Returns the table descriptor for (Dynamic)(BitmapTable)Get operators
//
//---------------------------------------------------------------------------
CTableDescriptor *
CLogical::PtabdescFromTableGet
	(
	COperator *pop
	)
{
	GPOS_ASSERT(NULL != pop);
	switch (pop->Eopid())
	{
		case CLogical::EopLogicalGet:
			return CLogicalGet::PopConvert(pop)->Ptabdesc();
		case CLogical::EopLogicalDynamicGet:
			return CLogicalDynamicGet::PopConvert(pop)->Ptabdesc();
		case CLogical::EopLogicalBitmapTableGet:
			return CLogicalBitmapTableGet::PopConvert(pop)->Ptabdesc();
		case CLogical::EopLogicalDynamicBitmapTableGet:
			return CLogicalDynamicBitmapTableGet::PopConvert(pop)->Ptabdesc();
		default:
			GPOS_ASSERT(false && "Unsupported operator in CLogical::PtabdescFromTableGet");
			return NULL;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CLogical::PdrgpcrOutputFromLogicalGet
//
//	@doc:
//		Extract the output columns from a logical get or dynamic get operator
//
//---------------------------------------------------------------------------
DrgPcr *
CLogical::PdrgpcrOutputFromLogicalGet
	(
	CLogical *pop
	)
{
	GPOS_ASSERT(NULL != pop);
	GPOS_ASSERT(COperator::EopLogicalGet == pop->Eopid() || COperator::EopLogicalDynamicGet == pop->Eopid());
	
	if (COperator::EopLogicalGet == pop->Eopid())
	{
		return CLogicalGet::PopConvert(pop)->PdrgpcrOutput();
	}
		
	return CLogicalDynamicGet::PopConvert(pop)->PdrgpcrOutput();
}

//---------------------------------------------------------------------------
//	@function:
//		CLogical::NameFromLogicalGet
//
//	@doc:
//		Extract the name from a logical get or dynamic get operator
//
//---------------------------------------------------------------------------
const CName &
CLogical::NameFromLogicalGet
	(
	CLogical *pop
	)
{
	GPOS_ASSERT(NULL != pop);
	GPOS_ASSERT(COperator::EopLogicalGet == pop->Eopid() || COperator::EopLogicalDynamicGet == pop->Eopid());
	
	if (COperator::EopLogicalGet == pop->Eopid())
	{
		return CLogicalGet::PopConvert(pop)->Name();
	}
		
	return CLogicalDynamicGet::PopConvert(pop)->Name();
}

//---------------------------------------------------------------------------
//	@function:
//		CLogical::PcrsDist
//
//	@doc:
//		Return the set of distribution columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogical::PcrsDist
	(
	IMemoryPool *pmp,
	const CTableDescriptor *ptabdesc,
	const DrgPcr *pdrgpcrOutput
	)
{
	GPOS_ASSERT(NULL != ptabdesc);
	GPOS_ASSERT(NULL != pdrgpcrOutput);

	const DrgPcoldesc *pdrgpcoldesc = ptabdesc->Pdrgpcoldesc();
	const DrgPcoldesc *pdrgpcoldescDist = ptabdesc->PdrgpcoldescDist();
	GPOS_ASSERT(NULL != pdrgpcoldesc);
	GPOS_ASSERT(NULL != pdrgpcoldescDist);
	GPOS_ASSERT(pdrgpcrOutput->UlLength() == pdrgpcoldesc->UlLength());


	// mapping base table columns to corresponding column references
	HMICr *phmicr = GPOS_NEW(pmp) HMICr(pmp);
	const ULONG ulCols = pdrgpcoldesc->UlLength();
	for (ULONG ul = 0; ul < ulCols; ul++)
	{
		CColumnDescriptor *pcoldesc = (*pdrgpcoldesc)[ul];
		CColRef *pcr = (*pdrgpcrOutput)[ul];
		phmicr->FInsert(GPOS_NEW(pmp) INT(pcoldesc->IAttno()), pcr);
	}

	CColRefSet *pcrsDist = GPOS_NEW(pmp) CColRefSet(pmp);
	const ULONG ulDistCols = pdrgpcoldescDist->UlLength();
	for (ULONG ul2 = 0; ul2 < ulDistCols; ul2++)
	{
		CColumnDescriptor *pcoldesc = (*pdrgpcoldescDist)[ul2];
		const INT iAttno = pcoldesc->IAttno();
		CColRef *pcrMapped = phmicr->PtLookup(&iAttno);
		GPOS_ASSERT(NULL != pcrMapped);
		pcrsDist->Include(pcrMapped);
	}

	// clean up
	phmicr->Release();

	return pcrsDist;
}

// EOF

