//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDouble.h
//
//	@doc:
//		Safe implementation of floating point numbers and operations.
//---------------------------------------------------------------------------
#ifndef GPOS_CDouble_H
#define GPOS_CDouble_H

#include "gpos/base.h"

#define GPOS_FP_ABS_MIN	(1e-250)	// min value: 4.94066e-324
#define GPOS_FP_ABS_MAX	(1e+250)	// max value: 8.98847e+307

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CDouble
	//
	//	@doc:
	//		Class representing floating-point numbers
	//
	//---------------------------------------------------------------------------
	class CDouble
	{
		private:

			// double-precision value
			DOUBLE m_d;

			// min absolute value allowed
			DOUBLE m_dMinAbs;

			// max absolute value allowed
			DOUBLE m_dMaxAbs;

			// check validity in ctor
			inline void CheckValidity();

			// assign value while maintaining current sign
			inline void SetSignedVal(DOUBLE dVal);

#ifdef GPOS_DEBUG
			// check if two instances are compatible
			inline BOOL FCompat(const CDouble &fp) const;
#endif // GPOS_DEBUG


		public:

			// ctors
			inline CDouble(DOUBLE d, DOUBLE dMinAbs = GPOS_FP_ABS_MIN, DOUBLE dMaxAbs = GPOS_FP_ABS_MAX);
			inline CDouble(ULLONG ul, DOUBLE dMinAbs = GPOS_FP_ABS_MIN, DOUBLE dMaxAbs = GPOS_FP_ABS_MAX);
			inline CDouble(ULONG ul, DOUBLE dMinAbs = GPOS_FP_ABS_MIN, DOUBLE dMaxAbs = GPOS_FP_ABS_MAX);
			inline CDouble(LINT ul, DOUBLE dMinAbs = GPOS_FP_ABS_MIN, DOUBLE dMaxAbs = GPOS_FP_ABS_MAX);
			inline CDouble(INT ul, DOUBLE dMinAbs = GPOS_FP_ABS_MIN, DOUBLE dMaxAbs = GPOS_FP_ABS_MAX);

			// dtor
			inline ~CDouble()
			{}

			// value accessor
			inline DOUBLE DVal() const
			{
				return m_d;
			}

			// assignment
			inline CDouble& operator=(const CDouble&);

			// arithmetic operators
			friend CDouble operator + (const CDouble &fpLeft, const CDouble &fpRight);
			friend CDouble operator - (const CDouble &fpLeft, const CDouble &fpRight);
			friend CDouble operator * (const CDouble &fpLeft, const CDouble &fpRight);
			friend CDouble operator / (const CDouble &fpLeft, const CDouble &fpRight);

			// negation
			friend CDouble operator - (const CDouble &fp);

			// logical operators
			friend BOOL operator == (const CDouble &fpLeft, const CDouble &fpRight);
			friend BOOL operator != (const CDouble &fpLeft, const CDouble &fpRight);
			friend BOOL operator < (const CDouble &fpLeft, const CDouble &fpRight);
			friend BOOL operator <= (const CDouble &fpLeft, const CDouble &fpRight);
			friend BOOL operator > (const CDouble &fpLeft, const CDouble &fpRight);
			friend BOOL operator >= (const CDouble &fpLeft, const CDouble &fpRight);

			// absolute
			inline CDouble FpAbs() const;

			// floor
			inline CDouble FpFloor() const;

			// ceiling
			inline CDouble FpCeil() const;

			// power
			inline CDouble FpPow(const CDouble &fp) const;

			// log to the base 2
			inline CDouble FpLog2() const;

			// print to stream
			inline IOstream &OsPrint
				(
				IOstream &os
				)
				const
			{
				return os << m_d;
			}

			// compare two double values using given precision
			inline static
			BOOL FEqual(DOUBLE dLeft, DOUBLE dRight, DOUBLE dPrecision = GPOS_FP_ABS_MIN);

	}; // class CDouble

	// arithmetic operators
	inline CDouble operator + (const CDouble &fpLeft, const CDouble &fpRight);
	inline CDouble operator - (const CDouble &fpLeft, const CDouble &fpRight);
	inline CDouble operator * (const CDouble &fpLeft, const CDouble &fpRight);
	inline CDouble operator / (const CDouble &fpLeft, const CDouble &fpRight);

	// logical operators
	inline BOOL operator == (const CDouble &fpLeft, const CDouble &fpRight);
	inline BOOL operator > (const CDouble &fpLeft, const CDouble &fpRight);

	// negation
	inline CDouble operator - (const CDouble &fp);

	// '!=' operator
	inline BOOL operator !=
		(
		const CDouble &fpLeft,
		const CDouble &fpRight
		)
	{
		return (!(fpLeft == fpRight));
	}

	// '<=' operator
	inline BOOL operator >=
		(
		const CDouble &fpLeft,
		const CDouble &fpRight
		)
	{
		return (fpLeft == fpRight || fpLeft > fpRight);
	}

	// '>' operator
	inline BOOL operator <
		(
		const CDouble &fpLeft,
		const CDouble &fpRight
		)
	{
		return (fpRight > fpLeft);
	}

	// '>=' operator
	inline BOOL operator <=
		(
		const CDouble &fpLeft,
		const CDouble &fpRight
		)
	{
		return (fpRight == fpLeft || fpRight > fpLeft);
	}

	// print shorthand
	inline
	IOstream & operator << (IOstream &os, CDouble fp)
	{
		return fp.OsPrint(os);
	}
}

// inlined implementation
#include "CDouble.inl"

#endif // !GPOS_CDouble_H

// EOF

