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


#include "gpos/base.h"
#include "gpos/common/clibwrapper.h"

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
	DOUBLE d,
	DOUBLE dMinAbs,
	DOUBLE dMaxAbs
	)
	:
	m_d(d),
	m_dMinAbs(dMinAbs),
	m_dMaxAbs(dMaxAbs)
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
	ULLONG ull,
	DOUBLE dMinAbs,
	DOUBLE dMaxAbs
	)
	:
	m_d(DOUBLE(ull)),
	m_dMinAbs(dMinAbs),
	m_dMaxAbs(dMaxAbs)
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
	ULONG ul,
	DOUBLE dMinAbs,
	DOUBLE dMaxAbs
	)
	:
	m_d(DOUBLE(ul)),
	m_dMinAbs(dMinAbs),
	m_dMaxAbs(dMaxAbs)
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
	LINT li,
	DOUBLE dMinAbs,
	DOUBLE dMaxAbs
	)
	:
	m_d(DOUBLE(li)),
	m_dMinAbs(dMinAbs),
	m_dMaxAbs(dMaxAbs)
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
	INT i,
	DOUBLE dMinAbs,
	DOUBLE dMaxAbs
	)
	:
	m_d(DOUBLE(i)),
	m_dMinAbs(dMinAbs),
	m_dMaxAbs(dMaxAbs)
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
	GPOS_ASSERT(0.0 < m_dMinAbs);
	GPOS_ASSERT(m_dMinAbs < m_dMaxAbs);

	double dAbs = clib::DAbs(m_d);

	if (m_dMaxAbs < dAbs)
	{
		SetSignedVal(m_dMaxAbs);
	}

	if (m_dMinAbs > dAbs)
	{
		SetSignedVal(m_dMinAbs);
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


#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CDouble::FCompat
//
//	@doc:
//		Check if two instances are compatible
//
//---------------------------------------------------------------------------
inline 
BOOL
CDouble::FCompat
	(
	const CDouble &fp
	)
	const
{
	return (CDouble::FEqual(this->m_dMinAbs, fp.m_dMinAbs) &&
	        CDouble::FEqual(this->m_dMaxAbs, fp.m_dMaxAbs));
}

#endif // GPOS_DEBUG


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
	GPOS_ASSERT(fpLeft.FCompat(fpRight));

	return CDouble(fpLeft.m_d + fpRight.m_d, fpLeft.m_dMinAbs, fpLeft.m_dMaxAbs);
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
	GPOS_ASSERT(fpLeft.FCompat(fpRight));

	return CDouble(fpLeft.m_d - fpRight.m_d, fpLeft.m_dMinAbs, fpLeft.m_dMaxAbs);
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
	GPOS_ASSERT(fpLeft.FCompat(fpRight));

	return CDouble(fpLeft.m_d * fpRight.m_d, fpLeft.m_dMinAbs, fpLeft.m_dMaxAbs);
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
	GPOS_ASSERT(fpLeft.FCompat(fpRight));

	return CDouble(fpLeft.m_d / fpRight.m_d, fpLeft.m_dMinAbs, fpLeft.m_dMaxAbs);
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
	return CDouble(-fp.m_d, fp.m_dMinAbs, fp.m_dMaxAbs);
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
	GPOS_ASSERT(fpLeft.FCompat(fpRight));

	CDouble fpCompare(fpLeft.m_d - fpRight.m_d, fpRight.m_dMinAbs, fpRight.m_dMaxAbs);

	return (clib::DAbs(fpCompare.m_d) == fpCompare.m_dMinAbs);
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
	GPOS_ASSERT(fpLeft.FCompat(fpRight));

	CDouble fpCompare(fpLeft.m_d - fpRight.m_d, fpRight.m_dMinAbs, fpRight.m_dMaxAbs);

	return (fpCompare.m_d > fpCompare.m_dMinAbs);
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
	return CDouble(clib::DAbs(m_d), m_dMinAbs, m_dMaxAbs);
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
	return CDouble(clib::DLog2(m_d), m_dMinAbs, m_dMaxAbs);
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
	return CDouble(clib::DFloor(m_d), m_dMinAbs, m_dMaxAbs);
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
	return CDouble(clib::DCeil(m_d), m_dMinAbs, m_dMaxAbs);
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
	GPOS_ASSERT(FCompat(fp));

	return CDouble(clib::DPow(m_d, fp.m_d), m_dMinAbs, m_dMaxAbs);
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
	return clib::DAbs(dRight - dLeft) <= dPrecision;
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
	GPOS_ASSERT(this->FCompat(fpRight));

	this->m_d = fpRight.m_d;

	return (*this);
}

} // namespace gpos
// EOF
