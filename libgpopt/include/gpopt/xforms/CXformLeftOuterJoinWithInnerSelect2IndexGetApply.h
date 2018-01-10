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
#include "gpopt/xforms/CXformJoin2IndexApplyBase.h"

namespace gpopt
{
	using namespace gpos;

	class CXformLeftOuterJoinWithInnerSelect2IndexGetApply : public CXformJoin2IndexApplyBase
		<CLogicalLeftOuterJoin, CLogicalLeftOuterIndexApply, CLogicalGet,
		true /*fWithSelect*/, false /*fPartial*/, IMDIndex::EmdindBtree>
	{
		private:
			// private copy ctor
			CXformLeftOuterJoinWithInnerSelect2IndexGetApply(const CXformLeftOuterJoinWithInnerSelect2IndexGetApply &);

		public:
			// ctor
			explicit
			CXformLeftOuterJoinWithInnerSelect2IndexGetApply(IMemoryPool *pmp)
				: CXformJoin2IndexApplyBase
				<CLogicalLeftOuterJoin, CLogicalLeftOuterIndexApply, CLogicalGet,
				true /*fWithSelect*/, false /*fPartial*/, IMDIndex::EmdindBtree>
				(pmp)
			{}

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
	};
}


#endif // !GPOPT_CXformLeftOuterJoinWithInnerSelect2IndexGetApply_H

// EOF
