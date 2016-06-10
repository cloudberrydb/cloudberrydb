//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CStatsPredLike.h
//
//	@doc:
//		LIKE filter for statistics
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CStatsPredLike_H
#define GPNAUCRATES_CStatsPredLike_H

#include "gpos/base.h"
#include "naucrates/md/IMDType.h"
#include "naucrates/statistics/CStatsPred.h"

// fwd decl
namespace gpopt
{
	class CExpression;
}

namespace gpnaucrates
{
	using namespace gpos;
	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		CStatsPredLike
	//
	//	@doc:
	//		LIKE filter for statistics
	//---------------------------------------------------------------------------
	class CStatsPredLike : public CStatsPred
	{
		private:

			// private copy ctor
			CStatsPredLike(const CStatsPredLike &);

			// private assignment operator
			CStatsPredLike& operator=(CStatsPredLike &);

			// left hand side of the LIKE expression
			CExpression *m_pexprLeft;

			// right hand side of the LIKE expression
			CExpression *m_pexprRight;

			// default scale factor
			CDouble m_dDefaultScaleFactor;

		public:

			// ctor
			CStatsPredLike
				(
				ULONG ulColId,
				CExpression *pexprLeft,
				CExpression *pexprRight,
				CDouble dDefaultScaleFactor
				);

			// dtor
			virtual
			~CStatsPredLike();

			// the column identifier on which the predicates are on
			virtual
			ULONG UlColId() const;

			// filter type id
			virtual
			EStatsPredType Espt() const
			{
				return CStatsPred::EsptLike;
			}

			// left hand side of the LIKE expression
			virtual
			CExpression *PexprLeft() const
			{
				return m_pexprLeft;
			}

			// right hand side of the LIKE expression
			virtual
			CExpression *PexprRight() const
			{
				return m_pexprRight;
			}

			// default scale factor
			virtual
			CDouble DDefaultScaleFactor() const;

			// conversion function
			static
			CStatsPredLike *PstatspredConvert
				(
				CStatsPred *pstatspred
				)
			{
				GPOS_ASSERT(NULL != pstatspred);
				GPOS_ASSERT(CStatsPred::EsptLike == pstatspred->Espt());

				return dynamic_cast<CStatsPredLike*>(pstatspred);
			}

	}; // class CStatsPredLike
}

#endif // !GPNAUCRATES_CStatsPredLike_H

// EOF
