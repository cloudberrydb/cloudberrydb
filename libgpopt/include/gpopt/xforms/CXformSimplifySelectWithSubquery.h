//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformSimplifySelectWithSubquery.h
//
//	@doc:
//		Simplify Select with subquery
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformSimplifySelectWithSubquery_H
#define GPOPT_CXformSimplifySelectWithSubquery_H

#include "gpos/base.h"
#include "gpopt/operators/CPatternTree.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CLogicalSelect.h"
#include "gpopt/xforms/CXformSimplifySubquery.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CXformSimplifySelectWithSubquery
	//
	//	@doc:
	//		Simplify Select with subquery
	//
	//---------------------------------------------------------------------------
	class CXformSimplifySelectWithSubquery : public CXformSimplifySubquery
	{

		private:

			// private copy ctor
			CXformSimplifySelectWithSubquery(const CXformSimplifySelectWithSubquery &);

		public:

			// ctor
			explicit
			CXformSimplifySelectWithSubquery
				(
				IMemoryPool *pmp
				)
				:
				// pattern
				CXformSimplifySubquery
				(
				GPOS_NEW(pmp) CExpression
						(
						pmp,
						GPOS_NEW(pmp) CLogicalSelect(pmp),
						GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)), // relational child
						GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp))	// predicate tree
						)
				)
			{}

			// dtor
			virtual
			~CXformSimplifySelectWithSubquery()
			{}

			// Compatibility function for simplifying aggregates
			virtual
			BOOL FCompatible
				(
				CXform::EXformId exfid
				)
			{
				return (CXform::ExfSimplifySelectWithSubquery != exfid);
			}

			// ident accessors
			virtual
			EXformId Exfid() const
			{
				return ExfSimplifySelectWithSubquery;
			}

			// return a string for xform name
			virtual
			const CHAR *SzId() const
			{
				return "CXformSimplifySelectWithSubquery";
			}

			// is transformation a subquery unnesting (Subquery To Apply) xform?
			virtual
			BOOL FSubqueryUnnesting() const
			{
				return true;
			}

	}; // class CXformSimplifySelectWithSubquery

}

#endif // !GPOPT_CXformSimplifySelectWithSubquery_H

// EOF
