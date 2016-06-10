//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		IMDCheckConstraint.h
//
//	@doc:
//		Interface class for check constraint in a metadata cache relation
//---------------------------------------------------------------------------

#ifndef GPMD_IMDCheckConstraint_H
#define GPMD_IMDCheckConstraint_H

#include "gpos/base.h"

#include "naucrates/md/IMDCacheObject.h"

#include "gpopt/base/CColRef.h"

// fwd decl
namespace gpdxl
{
	class CDXLNode;
}

namespace gpopt
{
	class CExpression;
	class CMDAccessor;
}

namespace gpmd
{
	using namespace gpos;
	using namespace gpopt;

	//---------------------------------------------------------------------------
	//	@class:
	//		IMDCheckConstraint
	//
	//	@doc:
	//		Interface class for check constraint in a metadata cache relation
	//
	//---------------------------------------------------------------------------
	class IMDCheckConstraint : public IMDCacheObject
	{
		public:

	 		// object type
	 	 	virtual
	 	 	Emdtype Emdt() const
	 	 	{
	 	 		return EmdtCheckConstraint;
	 	 	}

			// mdid of the relation
			virtual
			IMDId *PmdidRel() const = 0;

			// the scalar expression of the check constraint
			virtual
			CExpression *Pexpr(IMemoryPool *pmp, CMDAccessor *pmda, DrgPcr *pdrgpcr) const = 0;
	};
}

#endif // !GPMD_IMDCheckConstraint_H

// EOF
