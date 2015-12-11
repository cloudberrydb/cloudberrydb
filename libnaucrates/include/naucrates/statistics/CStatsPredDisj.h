//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CStatsPredDisj.h
//
//	@doc:
//		Disjunctive filter on statistics
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CStatsPredDisj_H
#define GPNAUCRATES_CStatsPredDisj_H

#include "gpos/base.h"
#include "naucrates/statistics/CPoint.h"
#include "naucrates/statistics/CStatsPred.h"

namespace gpnaucrates
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CStatsPredDisj
	//
	//	@doc:
	//		Disjunctive filter on statistics
	//---------------------------------------------------------------------------
	class CStatsPredDisj : public CStatsPred
	{
		private:

			// private copy ctor
			CStatsPredDisj(const CStatsPredDisj &);

			// private assignment operator
			CStatsPredDisj& operator=(CStatsPredDisj &);

			// array of filters
			DrgPstatspred *m_pdrgpstatspred;

		public:

			// ctor
			explicit
			CStatsPredDisj(DrgPstatspred *pdrgpstatspred);

			// dtor
			virtual
			~CStatsPredDisj()
			{
				m_pdrgpstatspred->Release();
			}

			// the column identifier on which the predicates are on
			virtual
			ULONG UlColId() const;

			// total number of predicates in the disjunction
			ULONG UlFilters() const
			{
				return m_pdrgpstatspred->UlLength();
			}

			// return the array of predicate filters
			DrgPstatspred *Pdrgpstatspred() const
			{
				return m_pdrgpstatspred;
			}

			// sort the components of the disjunction
			void Sort() const;

			// return the point filter at a particular position
			CStatsPred *Pstatspred(ULONG ulPos) const;

			// filter type id
			virtual
			EStatsPredType Espt() const
			{
				return CStatsPred::EsptDisj;
			}

			// return the column id of the filter based on the column ids of its child filters
			static
			ULONG UlColId(const DrgPstatspred *pdrgpstatspred);

			// conversion function
			static
			CStatsPredDisj *PstatspredConvert
				(
				CStatsPred *pstatspred
				)
			{
				GPOS_ASSERT(NULL != pstatspred);
				GPOS_ASSERT(CStatsPred::EsptDisj == pstatspred->Espt());

				return dynamic_cast<CStatsPredDisj*>(pstatspred);
			}

	}; // class CStatsPredDisj
}

#endif // !GPNAUCRATES_CStatsPredDisj_H

// EOF
