//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CCost.h
//
//	@doc:
//		Cost type
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CCost_H
#define GPNAUCRATES_CCost_H

#include "gpos/base.h"
#include "gpos/common/CDouble.h"
#include "gpos/common/CDynamicPtrArray.h"

namespace gpopt
{
	using namespace gpos;

	class CCost;
	typedef CDynamicPtrArray<CCost, CleanupDelete> DrgPcost;

	//---------------------------------------------------------------------------
	//	@class:
	//		CCost
	//
	//	@doc:
	//		Cost for comparing two plans
	//
	//---------------------------------------------------------------------------
	class CCost: public CDouble
	{
		public:

			// ctor
			explicit
			CCost
				(
				DOUBLE d
				)
				: CDouble(d)
			{}

			// ctor
			explicit
			CCost
				(
				CDouble d
				)
				: CDouble(d.DVal())
			{}

			// ctor
			CCost
				(
				const CCost &cost
				)
				: CDouble(cost.DVal())
			{}

			// assignment
			CCost& operator=
					(
					const CCost&cost
					)
			{
				*(CDouble *) (this) = (CDouble) cost;
				return (*this);
			}

			// addition operator
			CCost operator+
					(
					const CCost &cost
					)
					const
			{
				CDouble d = (CDouble) (*this) + (CDouble) cost;
				return CCost(d);
			}

			// multiplication operator
			CCost operator*
					(
					const CCost &cost
					)
					const
			{
				return CCost((CDouble) (*this) * (CDouble) cost);
			}

			// comparison operator
			BOOL operator<
					(
					const CCost &cost
					)
					const
			{
				return (CDouble) (*this) < (CDouble) cost;
			}

			// comparison operator
			BOOL operator>
					(
					const CCost &cost
					)
					const
			{
				return (CDouble) cost < (CDouble) (*this);
			}

			// d'tor
			virtual
			~CCost()
			{}

	}; // class CCost

}

#endif // !GPNAUCRATES_CCost_H

// EOF
