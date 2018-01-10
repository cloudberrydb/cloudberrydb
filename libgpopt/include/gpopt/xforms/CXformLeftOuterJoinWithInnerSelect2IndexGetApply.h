//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal, Inc.
//
//	Transform left outer Join with Select on the inner branch to IndexGet Apply
//
//---------------------------------------------------------------------------

#ifndef GPOPT_CXformLeftOuterJoinWithInnerSelect2IndexGetApply_H
#define GPOPT_CXformLeftOuterJoinWithInnerSelect2IndexGetApply_H

#include "gpos/base.h"
#include "gpopt/xforms/CXformLeftOuterJoin2IndexApply.h"

namespace gpopt
{
	using namespace gpos;

	class CXformLeftOuterJoinWithInnerSelect2IndexGetApply : public CXformLeftOuterJoin2IndexApply
	{
		private:
			// private copy ctor
			CXformLeftOuterJoinWithInnerSelect2IndexGetApply(const CXformLeftOuterJoinWithInnerSelect2IndexGetApply &);

		public:
			// ctor
			explicit
			CXformLeftOuterJoinWithInnerSelect2IndexGetApply(IMemoryPool *pmp);

			// dtor
			virtual
			~CXformLeftOuterJoinWithInnerSelect2IndexGetApply()
			{}

			// ident accessors
			virtual
			EXformId Exfid() const
			{
				return ExfLeftOuterJoinWithInnerSelect2IndexGetApply;
			}

			virtual
			const CHAR *SzId() const
			{
				return "CXformLeftOuterJoinWithInnerSelect2IndexGetApply";
			}

			// actual transform
			virtual
			void Transform(CXformContext *pxfctxt, CXformResult *pxfres, CExpression *pexpr) const;
	};
}


#endif // !GPOPT_CXformLeftOuterJoinWithInnerSelect2IndexGetApply_H

// EOF
