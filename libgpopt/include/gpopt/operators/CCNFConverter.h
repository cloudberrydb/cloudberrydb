//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright 2012 EMC Corp.
//
//	@filename:
//		CCNFConverter.h
//
//	@doc:
//		Convert scalar predicate to conjunctive normal form
//---------------------------------------------------------------------------
#ifndef GPOPT_CCNFConverter_H
#define GPOPT_CCNFConverter_H

#include "gpos/base.h"
#include "gpopt/operators/CExpression.h"

namespace gpopt
{

	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CCNFConverter
	//
	//	@doc:
	//		Convert scalar predicate to conjunctive normal form
	//
	//---------------------------------------------------------------------------
	class CCNFConverter
	{

		private:

			// does the given expression have a scalar bool op?
			static
			BOOL FScalarBoolOp(CExpression *pexpr);

			// convert an AND tree into CNF
			static
			CExpression *PexprAnd2CNF(IMemoryPool *pmp, CExpression *pexpr);

			// convert an OR tree into CNF
			static
			CExpression *PexprOr2CNF(IMemoryPool *pmp, CExpression *pexpr);

			// convert a NOT tree into CNF
			static
			CExpression *PexprNot2CNF(IMemoryPool *pmp, CExpression *pexpr);

			// call converter recursively on expression children
			static
			DrgPexpr *PdrgpexprConvertChildren(IMemoryPool *pmp, CExpression *pexpr);

			// convert children of expressions in the given array into an array of arrays
			static
			DrgPdrgPexpr *Pdrgpdrgpexpr(IMemoryPool *pmp, DrgPexpr *pdrgpexpr);

			// recursively compute cross product over the input array of arrays
			static
			void Expand
				(
				IMemoryPool *pmp,
				DrgPdrgPexpr *pdrgpdrgpexprInput,
				DrgPexpr *pdrgpexprOutput,
				DrgPexpr *pdrgpexprToExpand,
				ULONG ulCurrent
				);

			// private copy ctor
			CCNFConverter(const CCNFConverter &);

		public:

			// main driver
			static
			CExpression *Pexpr2CNF(IMemoryPool *pmp, CExpression *pexpr);

	}; // class CCNFConverter
}


#endif // !GPOPT_CCNFConverter_H

// EOF
