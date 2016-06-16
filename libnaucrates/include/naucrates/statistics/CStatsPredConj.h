//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CStatsPredConj.h
//
//	@doc:
//		Conjunctive filter on statistics
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CStatsPredConj_H
#define GPNAUCRATES_CStatsPredConj_H

#include "gpos/base.h"
#include "naucrates/statistics/CPoint.h"
#include "naucrates/statistics/CStatsPred.h"

namespace gpnaucrates
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CStatsPredConj
	//
	//	@doc:
	//		Conjunctive filter on statistics
	//---------------------------------------------------------------------------
	class CStatsPredConj : public CStatsPred
	{
		private:

			// private copy ctor
			CStatsPredConj(const CStatsPredConj &);

			// private assignment operator
			CStatsPredConj& operator=(CStatsPredConj &);

			// array of filters
			DrgPstatspred *m_pdrgpstatspred;

		public:

			// ctor
			explicit
			CStatsPredConj(DrgPstatspred *pdrgpstatspred);

			// dtor
			virtual
			~CStatsPredConj()
			{
				m_pdrgpstatspred->Release();
			}

			// the column identifier on which the predicates are on
			virtual
			ULONG UlColId() const;

			// total number of predicates in the conjunction
			ULONG UlFilters() const
			{
				return m_pdrgpstatspred->UlLength();
			}

			DrgPstatspred *Pdrgpstatspred() const
			{
				return m_pdrgpstatspred;
			}

			// sort the components of the conjunction
			void Sort() const;

			// return the filter at a particular position
			CStatsPred *Pstatspred(ULONG ulPos) const;

			// filter type id
			virtual
			EStatsPredType Espt() const
			{
				return CStatsPred::EsptConj;
			}

			// conversion function
			static
			CStatsPredConj *PstatspredConvert
				(
				CStatsPred *pstatspred
				)
			{
				GPOS_ASSERT(NULL != pstatspred);
				GPOS_ASSERT(CStatsPred::EsptConj == pstatspred->Espt());

				return dynamic_cast<CStatsPredConj*>(pstatspred);
			}

	}; // class CStatsPredConj
}

#endif // !GPNAUCRATES_CStatsPredConj_H

// EOF
