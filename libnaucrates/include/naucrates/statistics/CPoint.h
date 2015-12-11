//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPoint.h
//
//	@doc:
//		Point in the datum space
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CPoint_H
#define GPNAUCRATES_CPoint_H

#include "gpos/base.h"
#include "naucrates/base/IDatum.h"
#include "gpos/common/CDouble.h"

namespace gpopt
{
	class CMDAccessor;
}

namespace gpnaucrates
{
	using namespace gpos;
	using namespace gpmd;
	using namespace gpopt;

	//---------------------------------------------------------------------------
	//	@class:
	//		CPoint
	//
	//	@doc:
	//		One dimensional point in the datum space
	//---------------------------------------------------------------------------
	class CPoint: public CRefCount
	{
		private:

			// private copy ctor
			CPoint(const CPoint &);

			// private assignment operator
			CPoint& operator=(CPoint &);

			// datum corresponding to the point
			IDatum *m_pdatum;

		public:

			// c'tor
			explicit 
			CPoint(IDatum *);

			// get underlying datum
			IDatum *Pdatum() const
			{
				return m_pdatum;
			}

			// is this point equal to another
			BOOL FEqual(const CPoint *) const;

			// is this point not equal to another
			BOOL FNotEqual(const CPoint *) const;

			// less than
			BOOL FLessThan(const CPoint *) const;

			// less than or equals
			BOOL FLessThanOrEqual(const CPoint *) const;

			// greater than
			BOOL FGreaterThan(const CPoint *) const;

			// greater than or equals
			BOOL FGreaterThanOrEqual(const CPoint *) const;

			// distance between two points
			CDouble DDistance(const CPoint *) const;

			// print function
			virtual
			IOstream &OsPrint(IOstream &os) const;

			// d'tor
			virtual ~CPoint()
			{
				m_pdatum->Release();
			}

			// translate the point into its DXL representation
			CDXLDatum *Pdxldatum(IMemoryPool *pmp, CMDAccessor *pmda) const;

			// minimum of two points using <=
			static
			CPoint *PpointMin(CPoint *ppoint1, CPoint *ppoint2);

			// maximum of two points using >=
			static
			CPoint *PpointMax(CPoint *ppoint1, CPoint *ppoint2);
	}; // class CPoint

}

#endif // !GPNAUCRATES_CPoint_H

// EOF
