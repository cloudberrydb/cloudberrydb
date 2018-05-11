//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformSimplifyProjectWithSubquery.h
//
//	@doc:
//		Simplify Project with subquery
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformSimplifyProjectWithSubquery_H
#define GPOPT_CXformSimplifyProjectWithSubquery_H

#include "gpos/base.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CLogicalProject.h"
#include "gpopt/xforms/CXformSimplifySubquery.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CXformSimplifyProjectWithSubquery
	//
	//	@doc:
	//		Simplify Project with subquery
	//
	//---------------------------------------------------------------------------
	class CXformSimplifyProjectWithSubquery : public CXformSimplifySubquery
	{

		private:

			// private copy ctor
			CXformSimplifyProjectWithSubquery(const CXformSimplifyProjectWithSubquery &);

		public:

			// ctor
			explicit
			CXformSimplifyProjectWithSubquery
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
						GPOS_NEW(pmp) CLogicalProject(pmp),
						GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)),	// relational child
						GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp))	// project list
						)
				)
			{}

			// dtor
			virtual
			~CXformSimplifyProjectWithSubquery()
			{}

			// Compatibility function for simplifying aggregates
			virtual
			BOOL FCompatible
				(
				CXform::EXformId exfid
				)
			{
				return (CXform::ExfSimplifyProjectWithSubquery != exfid);
			}

			// ident accessors
			virtual
			EXformId Exfid() const
			{
				return ExfSimplifyProjectWithSubquery;
			}

			// return a string for xform name
			virtual
			const CHAR *SzId() const
			{
				return "CXformSimplifyProjectWithSubquery";
			}

			// is transformation a subquery unnesting (Subquery To Apply) xform?
			virtual
			BOOL FSubqueryUnnesting() const
			{
				return true;
			}

	}; // class CXformSimplifyProjectWithSubquery

}

#endif // !GPOPT_CXformSimplifyProjectWithSubquery_H

// EOF
