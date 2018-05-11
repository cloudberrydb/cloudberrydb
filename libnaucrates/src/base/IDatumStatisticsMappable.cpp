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
//		IDatumStatisticsMappable::StatsAreEqual
//
//	@doc:
//		Equality based on mapping to LINT or CDouble
//
//---------------------------------------------------------------------------
BOOL 
IDatumStatisticsMappable::StatsAreEqual
		(
				const IDatum *datum
		)
	const
{
	GPOS_ASSERT(NULL != datum);
	
	const IDatumStatisticsMappable *datum_cast = dynamic_cast<const IDatumStatisticsMappable*>(datum);

	// datums can be compared based on either LINT or Doubles or BYTEA values
#ifdef GPOS_DEBUG
	BOOL is_double_comparison = this->IsDatumMappableToDouble() && datum_cast->IsDatumMappableToDouble();
#endif // GPOS_DEBUG
	BOOL is_lint_comparison = this->IsDatumMappableToLINT() && datum_cast->IsDatumMappableToLINT();
	BOOL is_binary_comparison = this->SupportsBinaryComp(datum) && datum_cast->SupportsBinaryComp(this);

	GPOS_ASSERT(is_double_comparison || is_lint_comparison || is_binary_comparison);

	if (this->IsNull())
	{
		// nulls are equal from stats point of view
		return datum_cast->IsNull();
	}

	if (datum_cast->IsNull())
	{
		return false;
	}

	if (is_binary_comparison)
	{
		return StatsEqualBinary(datum);
	}

	if (is_lint_comparison)
	{
		LINT l1 = this->GetLINTMapping();
		LINT l2 = datum_cast->GetLINTMapping();
		return l1 == l2;
	}

	GPOS_ASSERT(is_double_comparison);

	CDouble d1 = this->GetDoubleMapping();
	CDouble d2 = datum_cast->GetDoubleMapping();
	return d1 == d2;
}

//---------------------------------------------------------------------------
//	@function:
//		IDatumStatisticsMappable::StatsAreLessThan
//
//	@doc:
//		Less-than based on mapping to LINT or CDouble
//
//---------------------------------------------------------------------------
BOOL 
IDatumStatisticsMappable::StatsAreLessThan
	(
	const IDatum *datum
	)
	const
{
	GPOS_ASSERT(NULL != datum);
	
	const IDatumStatisticsMappable *datum_cast = dynamic_cast<const IDatumStatisticsMappable*>(datum);

	// datums can be compared based on either LINT or Doubles or BYTEA values
#ifdef GPOS_DEBUG
	BOOL is_double_comparison = this->IsDatumMappableToDouble() && datum_cast->IsDatumMappableToDouble();
#endif // GPOS_DEBUG
	BOOL is_lint_comparison = this->IsDatumMappableToLINT() && datum_cast->IsDatumMappableToLINT();
	BOOL is_binary_comparison = this->SupportsBinaryComp(datum) && datum_cast->SupportsBinaryComp(this);

	GPOS_ASSERT(is_double_comparison || is_lint_comparison || is_binary_comparison);

	if (this->IsNull())
	{
		// nulls are less than everything else except nulls
		return !(datum_cast->IsNull());
	}

	if (datum_cast->IsNull())
	{
		return false;
	}

	if (is_binary_comparison)
	{
		return StatsLessThanBinary(datum);
	}

	if (is_lint_comparison)
	{
		LINT l1 = this->GetLINTMapping();
		LINT l2 = datum_cast->GetLINTMapping();
		return l1 < l2;
	}

	GPOS_ASSERT(is_double_comparison);

	CDouble d1 = this->GetDoubleMapping();
	CDouble d2 = datum_cast->GetDoubleMapping();
	return d1 < d2;
}

//---------------------------------------------------------------------------
//	@function:
//		IDatumStatisticsMappable::GetStatsDistanceFrom
//
//	@doc:
//		Distance function based on mapping to LINT or CDouble
//
//---------------------------------------------------------------------------
CDouble 
IDatumStatisticsMappable::GetStatsDistanceFrom
	(
	const IDatum *datum
	)
	const
{
	GPOS_ASSERT(NULL != datum);

	const IDatumStatisticsMappable *datum_cast = dynamic_cast<const IDatumStatisticsMappable*>(datum);

	// datums can be compared based on either LINT or Doubles or BYTEA values
#ifdef GPOS_DEBUG
	BOOL is_double_comparison = this->IsDatumMappableToDouble() && datum_cast->IsDatumMappableToDouble();
#endif // GPOS_DEBUG
	BOOL is_lint_comparison = this->IsDatumMappableToLINT() && datum_cast->IsDatumMappableToLINT();
	BOOL is_binary_comparison = this->SupportsBinaryComp(datum) && datum_cast->SupportsBinaryComp(this);

	GPOS_ASSERT(is_double_comparison || is_lint_comparison || is_binary_comparison);

	if (this->IsNull())
	{
		// nulls are equal from stats point of view
		return datum_cast->IsNull();
	}

	if (datum_cast->IsNull())
	{
		return false;
	}

	if (is_binary_comparison)
	{
		// TODO: , May 1 2013, distance function for data types such as bpchar/varchar
		// that require binary comparison
		LINT l1 = this->GetLINTMapping();
		LINT l2 = datum_cast->GetLINTMapping();

		return fabs(CDouble(l1 - l2).Get());
	}

	if (is_lint_comparison)
	{
		LINT l1 = this->GetLINTMapping();
		LINT l2 = datum_cast->GetLINTMapping();
		return l1 - l2;
	}

	GPOS_ASSERT(is_double_comparison);

	CDouble d1 = this->GetDoubleMapping();
	CDouble d2 = datum_cast->GetDoubleMapping();
	return d1 - d2;
}

//---------------------------------------------------------------------------
//	@function:
//		IDatumStatisticsMappable::GetValAsDouble
//
//	@doc:
//		 Return double representation of mapping value
//
//---------------------------------------------------------------------------
CDouble
IDatumStatisticsMappable::GetValAsDouble() const
{
	if (IsNull())
	{
		return CDouble(0.0);
	}

	if (IsDatumMappableToLINT())
	{
		return CDouble(GetLINTMapping());
	}

	return CDouble(GetDoubleMapping());
}


//---------------------------------------------------------------------------
//	@function:
//		IDatumStatisticsMappable::StatsAreComparable
//
//	@doc:
//		Check if the given pair of datums are stats comparable
//
//---------------------------------------------------------------------------
BOOL
IDatumStatisticsMappable::StatsAreComparable
	(
	const IDatum *datum
	)
	const
{
	GPOS_ASSERT(NULL != datum);

	const IDatumStatisticsMappable *datum_cast = dynamic_cast<const IDatumStatisticsMappable*>(datum);

	// datums can be compared based on either LINT or Doubles or BYTEA values
	BOOL is_double_comparison = this->IsDatumMappableToDouble() && datum_cast->IsDatumMappableToDouble();
	BOOL is_lint_comparison = this->IsDatumMappableToLINT() && datum_cast->IsDatumMappableToLINT();
	BOOL is_binary_comparison = this->SupportsBinaryComp(datum_cast) && datum_cast->SupportsBinaryComp(this);

	return is_double_comparison || is_lint_comparison || is_binary_comparison;
}

//EOF
