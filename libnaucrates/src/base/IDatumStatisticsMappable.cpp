//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		IDatumStatisticsMappable.cpp
//
//	@doc:
//
//---------------------------------------------------------------------------

#include "naucrates/base/IDatumStatisticsMappable.h"

using namespace gpnaucrates;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		IDatumStatisticsMappable::FStatsEqual
//
//	@doc:
//		Equality based on mapping to LINT or CDouble
//
//---------------------------------------------------------------------------
BOOL 
IDatumStatisticsMappable::FStatsEqual
	(
	const IDatum *pdatum
	)
	const
{
	GPOS_ASSERT(NULL != pdatum);
	
	const IDatumStatisticsMappable *pdatumsm = dynamic_cast<const IDatumStatisticsMappable*>(pdatum);

	// datums can be compared based on either LINT or Doubles or BYTEA values
#ifdef GPOS_DEBUG
	BOOL fDoubleComparison = this->FHasStatsDoubleMapping() && pdatumsm->FHasStatsDoubleMapping();
#endif // GPOS_DEBUG
	BOOL fLintComparison = this->FHasStatsLINTMapping() && pdatumsm->FHasStatsLINTMapping();
	BOOL fBinaryComparison = this->FSupportsBinaryComp(pdatum) && pdatumsm->FSupportsBinaryComp(this);

	GPOS_ASSERT(fDoubleComparison || fLintComparison || fBinaryComparison);

	if (this->FNull())
	{
		// nulls are equal from stats point of view
		return pdatumsm->FNull();
	}

	if (pdatumsm->FNull())
	{
		return false;
	}

	if (fBinaryComparison)
	{
		return FStatsEqualBinary(pdatum);
	}

	if (fLintComparison)
	{
		LINT l1 = this->LStatsMapping();
		LINT l2 = pdatumsm->LStatsMapping();
		return l1 == l2;
	}

	GPOS_ASSERT(fDoubleComparison);

	CDouble d1 = this->DStatsMapping();
	CDouble d2 = pdatumsm->DStatsMapping();
	return d1 == d2;
}

//---------------------------------------------------------------------------
//	@function:
//		IDatumStatisticsMappable::FStatsLessThan
//
//	@doc:
//		Less-than based on mapping to LINT or CDouble
//
//---------------------------------------------------------------------------
BOOL 
IDatumStatisticsMappable::FStatsLessThan
	(
	const IDatum *pdatum
	)
	const
{
	GPOS_ASSERT(NULL != pdatum);
	
	const IDatumStatisticsMappable *pdatumsm = dynamic_cast<const IDatumStatisticsMappable*>(pdatum);

	// datums can be compared based on either LINT or Doubles or BYTEA values
#ifdef GPOS_DEBUG
	BOOL fDoubleComparison = this->FHasStatsDoubleMapping() && pdatumsm->FHasStatsDoubleMapping();
#endif // GPOS_DEBUG
	BOOL fLintComparison = this->FHasStatsLINTMapping() && pdatumsm->FHasStatsLINTMapping();
	BOOL fBinaryComparison = this->FSupportsBinaryComp(pdatum) && pdatumsm->FSupportsBinaryComp(this);

	GPOS_ASSERT(fDoubleComparison || fLintComparison || fBinaryComparison);

	if (this->FNull())
	{
		// nulls are less than everything else except nulls
		return !(pdatumsm->FNull());
	}

	if (pdatumsm->FNull())
	{
		return false;
	}

	if (fBinaryComparison)
	{
		return FStatsLessThanBinary(pdatum);
	}

	if (fLintComparison)
	{
		LINT l1 = this->LStatsMapping();
		LINT l2 = pdatumsm->LStatsMapping();
		return l1 < l2;
	}

	GPOS_ASSERT(fDoubleComparison);

	CDouble d1 = this->DStatsMapping();
	CDouble d2 = pdatumsm->DStatsMapping();
	return d1 < d2;
}

//---------------------------------------------------------------------------
//	@function:
//		IDatumStatisticsMappable::DStatsDistance
//
//	@doc:
//		Distance function based on mapping to LINT or CDouble
//
//---------------------------------------------------------------------------
CDouble 
IDatumStatisticsMappable::DStatsDistance
	(
	const IDatum *pdatum
	)
	const
{
	GPOS_ASSERT(NULL != pdatum);

	const IDatumStatisticsMappable *pdatumsm = dynamic_cast<const IDatumStatisticsMappable*>(pdatum);

	// datums can be compared based on either LINT or Doubles or BYTEA values
#ifdef GPOS_DEBUG
	BOOL fDoubleComparison = this->FHasStatsDoubleMapping() && pdatumsm->FHasStatsDoubleMapping();
#endif // GPOS_DEBUG
	BOOL fLintComparison = this->FHasStatsLINTMapping() && pdatumsm->FHasStatsLINTMapping();
	BOOL fBinaryComparison = this->FSupportsBinaryComp(pdatum) && pdatumsm->FSupportsBinaryComp(this);

	GPOS_ASSERT(fDoubleComparison || fLintComparison || fBinaryComparison);

	if (this->FNull())
	{
		// nulls are equal from stats point of view
		return pdatumsm->FNull();
	}

	if (pdatumsm->FNull())
	{
		return false;
	}

	if (fBinaryComparison)
	{
		// TODO: , May 1 2013, distance function for data types such as bpchar/varchar
		// that require binary comparison
		LINT l1 = this->LStatsMapping();
		LINT l2 = pdatumsm->LStatsMapping();

		return fabs(CDouble(l1 - l2).DVal());
	}

	if (fLintComparison)
	{
		LINT l1 = this->LStatsMapping();
		LINT l2 = pdatumsm->LStatsMapping();
		return l1 - l2;
	}

	GPOS_ASSERT(fDoubleComparison);

	CDouble d1 = this->DStatsMapping();
	CDouble d2 = pdatumsm->DStatsMapping();
	return d1 - d2;
}

//---------------------------------------------------------------------------
//	@function:
//		IDatumStatisticsMappable::DMappingVal
//
//	@doc:
//		 Return double representation of mapping value
//
//---------------------------------------------------------------------------
CDouble
IDatumStatisticsMappable::DMappingVal() const
{
	if (FNull())
	{
		return CDouble(0.0);
	}

	if (FHasStatsLINTMapping())
	{
		return CDouble(LStatsMapping());
	}

	return CDouble(DStatsMapping());
}


//---------------------------------------------------------------------------
//	@function:
//		IDatumStatisticsMappable::FStatsComparable
//
//	@doc:
//		Check if the given pair of datums are stats comparable
//
//---------------------------------------------------------------------------
BOOL
IDatumStatisticsMappable::FStatsComparable
	(
	const IDatum *pdatum
	)
	const
{
	GPOS_ASSERT(NULL != pdatum);

	const IDatumStatisticsMappable *pdatumsm = dynamic_cast<const IDatumStatisticsMappable*>(pdatum);

	// datums can be compared based on either LINT or Doubles or BYTEA values
	BOOL fDoubleComparison = this->FHasStatsDoubleMapping() && pdatumsm->FHasStatsDoubleMapping();
	BOOL fLintComparison = this->FHasStatsLINTMapping() && pdatumsm->FHasStatsLINTMapping();
	BOOL fBinaryComparison = this->FSupportsBinaryComp(pdatumsm) && pdatumsm->FSupportsBinaryComp(this);

	return fDoubleComparison || fLintComparison || fBinaryComparison;
}

//EOF
