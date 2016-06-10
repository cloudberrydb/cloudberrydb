//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLPhysicalJoin.h
//
//	@doc:
//		Base class for representing physical DXL join operators.
//---------------------------------------------------------------------------



#ifndef GPDXL_CDXLPhysicalJoin_H
#define GPDXL_CDXLPhysicalJoin_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLPhysical.h"

namespace gpdxl
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLPhysicalJoin
	//
	//	@doc:
	//		Base class for representing physical DXL join operators
	//
	//---------------------------------------------------------------------------
	class CDXLPhysicalJoin : public CDXLPhysical
	{
		private:
			// private copy ctor
			CDXLPhysicalJoin(const CDXLPhysicalJoin&);

			// join type (inner, outer, ...)
			EdxlJoinType m_edxljt;
			
		public:
			// ctor
			CDXLPhysicalJoin(IMemoryPool *pmp, EdxlJoinType edxljt);
			
			// join type
			EdxlJoinType Edxltype() const;
			
			const CWStringConst *PstrJoinTypeName() const;

	};
}
#endif // !GPDXL_CDXLPhysicalJoin_H

// EOF

