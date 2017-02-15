//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDouble.cpp
//
//	@doc:
//		Implementation of floating point operations.
//---------------------------------------------------------------------------

#include <math.h>

#include "gpos/base.h"

namespace gpos
{


//---------------------------------------------------------------------------
//	@function:
//		CDouble::CDouble
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
inline 
CDouble::CDouble
	(
	DOUBLE d
	)
	:
	m_d(d)
{
	CheckValidity();
}


//---------------------------------------------------------------------------
//	@function:
//		CDouble::CDouble
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
inline 
CDouble::CDouble
	(
	ULLONG ull
	)
	:
	m_d(DOUBLE(ull))
{
	CheckValidity();
}


//---------------------------------------------------------------------------
//	@function:
//		CDouble::CDouble
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
inline 
CDouble::CDouble
	(
	ULONG ul
	)
	:
	m_d(DOUBLE(ul))
{
	CheckValidity();
}


//---------------------------------------------------------------------------
//	@function:
//		CDouble::CDouble
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
inline 
CDouble::CDouble
	(
	LINT li
	)
	:
	m_d(DOUBLE(li))
{
	CheckValidity();
}


//---------------------------------------------------------------------------
//	@function:
//		CDouble::CDouble
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
inline 
CDouble::CDouble
	(
	INT i
	)
	:
	m_d(DOUBLE(i))
{
	CheckValidity();
}


//---------------------------------------------------------------------------
//	@function:
//		CDouble::CheckValidity
//
//	@doc:
//		Check validity in ctor
//
//---------------------------------------------------------------------------
inline 
void
CDouble::CheckValidity()
{
	GPOS_ASSERT(0.0 < GPOS_FP_ABS_MIN);
	GPOS_ASSERT(GPOS_FP_ABS_MIN < GPOS_FP_ABS_MAX);

	double dAbs = fabs(m_d);

	if (GPOS_FP_ABS_MAX < dAbs)
	{
		SetSignedVal(GPOS_FP_ABS_MAX);
	}

	if (GPOS_FP_ABS_MIN > dAbs)
	{
		SetSignedVal(GPOS_FP_ABS_MIN);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CDouble::SetSignedVal
//
//	@doc:
//		Assign value while maintaining current sign
//
//---------------------------------------------------------------------------
inline 
void
CDouble::SetSignedVal(DOUBLE dVal)
{
	if (0.0 <= m_d)
	{
		m_d = dVal;
	}
	else
	{
		m_d = -dVal;
	}
}


//---------------------------------------------------------------------------
//	@function:
//		operator +
//
//	@doc:
//		Add two floating-point numbers
//
//---------------------------------------------------------------------------
inline 
CDouble
operator +
	(
	const CDouble &fpLeft,
	const CDouble &fpRight
	)
{
	return CDouble(fpLeft.m_d + fpRight.m_d);
}


//---------------------------------------------------------------------------
//	@function:
//		operator -
//
//	@doc:
//		Subtract two floating-point numbers
//
//---------------------------------------------------------------------------
inline 
CDouble
operator -
	(
	const CDouble &fpLeft,
	const CDouble &fpRight
	)
{
	return CDouble(fpLeft.m_d - fpRight.m_d);
}


//---------------------------------------------------------------------------
//	@function:
//		operator *
//
//	@doc:
//		Multiply two floating-point numbers
//
//---------------------------------------------------------------------------
inline 
CDouble
operator *
	(
	const CDouble &fpLeft,
	const CDouble &fpRight
	)
{
	return CDouble(fpLeft.m_d * fpRight.m_d);
}


//---------------------------------------------------------------------------
//	@function:
//		operator /
//
//	@doc:
//		Divide two floating-point numbers
//
//---------------------------------------------------------------------------
inline 
CDouble
operator /
	(
	const CDouble &fpLeft,
	const CDouble &fpRight
	)
{
	return CDouble(fpLeft.m_d / fpRight.m_d);
}


//---------------------------------------------------------------------------
//	@function:
//		operator -
//
//	@doc:
//		Negation
//
//---------------------------------------------------------------------------
inline 
CDouble
operator - (const CDouble &fp)
{
	return CDouble(-fp.m_d);
}


//---------------------------------------------------------------------------
//	@function:
//		operator ==
//
//	@doc:
//		Check if two floating-point numbers are equal
//
//---------------------------------------------------------------------------
inline 
BOOL
operator ==
	(
	const CDouble &fpLeft,
	const CDouble &fpRight
	)
{
	CDouble fpCompare(fpLeft.m_d - fpRight.m_d);

	return (fabs(fpCompare.m_d) == GPOS_FP_ABS_MIN);
}


//---------------------------------------------------------------------------
//	@function:
//		operator >
//
//	@doc:
//		Check if left floating-point number is greater than right one
//
//---------------------------------------------------------------------------
inline 
BOOL
operator >
	(
	const CDouble &fpLeft,
	const CDouble &fpRight
	)
{
	CDouble fpCompare(fpLeft.m_d - fpRight.m_d);

	return (fpCompare.m_d > GPOS_FP_ABS_MIN);
}


//---------------------------------------------------------------------------
//	@function:
//		CDouble::FpAbs
//
//	@doc:
//		Return absolute value
//
//---------------------------------------------------------------------------
inline 
CDouble
CDouble::FpAbs() const
{
	return CDouble(fabs(m_d));
}

//---------------------------------------------------------------------------
//	@function:
//		CDouble::FpLog2
//
//	@doc:
//		Log to the base 2
//
//---------------------------------------------------------------------------
inline 
CDouble
CDouble::FpLog2() const
{
	return CDouble(log2(m_d));
}

//---------------------------------------------------------------------------
//	@function:
//		CDouble::FpFloor
//
//	@doc:
//		Return floor value
//
//---------------------------------------------------------------------------
inline 
CDouble
CDouble::FpFloor() const
{
	return CDouble(floor(m_d));
}


//---------------------------------------------------------------------------
//	@function:
//		CDouble::FpCeil
//
//	@doc:
//		Return ceiling value
//
//---------------------------------------------------------------------------
inline 
CDouble
CDouble::FpCeil() const
{
	return CDouble(ceil(m_d));
}


//---------------------------------------------------------------------------
//	@function:
//		CDouble::FpPow
//
//	@doc:
//		Return power value
//
//---------------------------------------------------------------------------
inline 
CDouble
CDouble::FpPow
	(
	const CDouble &fp
	)
	const
{
	return CDouble(pow(m_d, fp.m_d));
}


//---------------------------------------------------------------------------
//	@function:
//		CDouble::FEqual
//
//	@doc:
//		Check if two double values are equal for given precision
//
//---------------------------------------------------------------------------
inline 
BOOL
CDouble::FEqual
	(
	DOUBLE dLeft,
	DOUBLE dRight,
	DOUBLE dPrecision
	)
{
	return fabs(dRight - dLeft) <= dPrecision;
}

//---------------------------------------------------------------------------
//	@function:
//		assignment operator =
//
//	@doc:
//		copy over value from another instance
//
//---------------------------------------------------------------------------
inline 
CDouble&
CDouble::operator =
	(
	const CDouble &fpRight
	)
{
	this->m_d = fpRight.m_d;

	return (*this);
}

} // namespace gpos
// EOF
