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

#include <math.h>

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

			// check validity in ctor
			inline void CheckValidity()
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

			// assign value while maintaining current sign
			inline void SetSignedVal(DOUBLE dVal)
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


		public:

			// ctors
			inline CDouble(DOUBLE d)
            :
            m_d(d)
            {
                CheckValidity();
            }

			inline CDouble(ULLONG ull)
            :
            m_d(DOUBLE(ull))
            {
                CheckValidity();
            }

			inline CDouble(ULONG ul)
            :
            m_d(DOUBLE(ul))
            {
                CheckValidity();
            }

			inline CDouble(LINT li)
            :
            m_d(DOUBLE(li))
            {
                CheckValidity();
            }

			inline CDouble(INT i)
            :
            m_d(DOUBLE(i))
            {
                CheckValidity();
            }

			// dtor
			inline ~CDouble()
			{}

			// value accessor
			inline DOUBLE DVal() const
			{
				return m_d;
			}

			// assignment
			inline CDouble& operator=(const CDouble &fpRight)
            {
                this->m_d = fpRight.m_d;

                return (*this);
            }

			// arithmetic operators
			friend CDouble operator + (const CDouble &fpLeft, const CDouble &fpRight)
            {
                return CDouble(fpLeft.m_d + fpRight.m_d);
            }

			friend CDouble operator - (const CDouble &fpLeft, const CDouble &fpRight)
            {
                return CDouble(fpLeft.m_d - fpRight.m_d);
            }

			friend CDouble operator * (const CDouble &fpLeft, const CDouble &fpRight)
            {
                return CDouble(fpLeft.m_d * fpRight.m_d);
            }

			friend CDouble operator / (const CDouble &fpLeft, const CDouble &fpRight)
            {
                return CDouble(fpLeft.m_d / fpRight.m_d);
            }

			// negation
			friend CDouble operator - (const CDouble &fp)
            {
                return CDouble(-fp.m_d);
            }

			// logical operators
			friend BOOL operator == (const CDouble &fpLeft, const CDouble &fpRight)
            {
                CDouble fpCompare(fpLeft.m_d - fpRight.m_d);

                return (fabs(fpCompare.m_d) == GPOS_FP_ABS_MIN);
            }

			friend BOOL operator != (const CDouble &fpLeft, const CDouble &fpRight);
			friend BOOL operator < (const CDouble &fpLeft, const CDouble &fpRight);
			friend BOOL operator <= (const CDouble &fpLeft, const CDouble &fpRight);
			friend BOOL operator > (const CDouble &fpLeft, const CDouble &fpRight)
            {
                CDouble fpCompare(fpLeft.m_d - fpRight.m_d);

                return (fpCompare.m_d > GPOS_FP_ABS_MIN);
            }

			friend BOOL operator >= (const CDouble &fpLeft, const CDouble &fpRight);

			// absolute
			inline CDouble FpAbs() const
            {
                return CDouble(fabs(m_d));
            }

			// floor
			inline CDouble FpFloor() const
            {
                return CDouble(floor(m_d));
            }

			// ceiling
			inline CDouble FpCeil() const
            {
                return CDouble(ceil(m_d));
            }

			// power
			inline CDouble FpPow(const CDouble &fp) const
            {
                return CDouble(pow(m_d, fp.m_d));
            }

			// log to the base 2
			inline CDouble FpLog2() const
            {
                return CDouble(log2(m_d));
            }

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
			BOOL FEqual(DOUBLE dLeft, DOUBLE dRight, DOUBLE dPrecision = GPOS_FP_ABS_MIN)
            {
                return fabs(dRight - dLeft) <= dPrecision;
            }

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

#endif // !GPOS_CDouble_H

// EOF

