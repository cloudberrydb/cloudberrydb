//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CBinding.h
//
//	@doc:
//		Binding mechanism to extract expression from Memo according to pattern
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CBinding_H
#define GPOPT_CBinding_H

#include "gpopt/operators/CExpression.h"

#include "gpos/base.h"

namespace gpopt
{
	using namespace gpos;

	// fwd declaration
	class CGroupExpression;
	class CGroup;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CBinding
	//
	//	@doc:
	//		Binding class used to iteratively generate expressions from the
	//		memo so that they match a given pattern
	//
	//
	//---------------------------------------------------------------------------
	class CBinding
	{
	
		private:
					
			// initialize cursors of child expressions
			BOOL FInitChildCursors
				(
				IMemoryPool *pmp,
				CGroupExpression *pgexpr,
				CExpression *pexprPattern,
				DrgPexpr *pdrgpexpr
				);

			// advance cursors of child expressions
			BOOL FAdvanceChildCursors
				(
				IMemoryPool *pmp,
				CGroupExpression *pgexpr,
				CExpression *pexprPattern,
				CExpression *pexprLast,
				DrgPexpr *pdrgpexpr
				);

			// extraction of child expressions
			BOOL FExtractChildren
				(
				IMemoryPool *pmp,
				CExpression *pexprPattern,
				CGroupExpression *pgexprCursor,
				DrgPexpr *pdrgpexpr
				);
			
			// move cursor
			CGroupExpression *PgexprNext
				(
				CGroup *pgroup,
				CGroupExpression *pgexpr
				)
				const;
			
			// expand n-th child of pattern
			CExpression *PexprExpandPattern
				(
				CExpression *pexpr,
				ULONG ulPos,
				ULONG ulArity
				);
			
			// get binding for children
			BOOL FExtractChildren
				(
				IMemoryPool *pmp,
				CGroupExpression *pgexpr,
				CExpression *pexprPattern,
				CExpression *pexprLast,
				DrgPexpr *pdrgpexprChildren
				);
			
			// extract binding from a group
			CExpression *PexprExtract
				(
				IMemoryPool *pmp,
				CGroup *pgroup,
				CExpression *pexprPattern,
				CExpression *pexprLast
				);
			
			// build expression
			CExpression *PexprFinalize
				(
				IMemoryPool *pmp,
				CGroupExpression *pgexpr,
				DrgPexpr *pdrgpexprChildren
				);
			
			// private copy ctor
			CBinding(const CBinding &);
						
		public:
		
			// ctor
			CBinding()
			{}
			
			// dtor
			~CBinding()
			{}
			
			// extract binding from group expression
			CExpression *PexprExtract
				(
				IMemoryPool *pmp,
				CGroupExpression *pgexpr,
				CExpression *pexprPatetrn,
				CExpression *pexprLast
				);

	}; // class CBinding
	
}

#endif // !GPOPT_CBinding_H

// EOF
