//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CScalar.h
//
//	@doc:
//		Base class for all scalar operators
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalar_H
#define GPOPT_CScalar_H

#include "gpos/base.h"
#include "gpopt/base/CDrvdProp.h"
#include "gpopt/base/CPartInfo.h"

#include "gpopt/mdcache/CMDAccessor.h"
#include "gpopt/operators/COperator.h"


#include "naucrates/md/IMDId.h"

namespace gpopt
{
	using namespace gpos;
	using namespace gpmd;

	// forward declaration
	class CColRefSet;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CScalar
	//
	//	@doc:
	//		base class for all scalar operators
	//
	//---------------------------------------------------------------------------
	class CScalar : public COperator
	{
		public:

			// possible results of Boolean evaluation of a scalar expression
			enum EBoolEvalResult
				{
				EberTrue = 1,	// TRUE
				EberFalse,		// FALSE
				EberNull,		// NULL
				EberUnknown,	// Unknown

				EerSentinel
				};

		private:

			// private copy ctor
			CScalar(const CScalar &);
			
			// helper for combining partition consumer arrays of scalar children
			static
			CPartInfo *PpartinfoDeriveCombineScalar(IMemoryPool *pmp, CExpressionHandle &exprhdl);

		protected:

			// perform conjunction of child boolean evaluation results
			static
			EBoolEvalResult EberConjunction(DrgPul *pdrgpulChildren);

			// perform disjunction of child boolean evaluation results
			static
			EBoolEvalResult EberDisjunction(DrgPul *pdrgpulChildren);

			// return Null if any child is Null
			static
			EBoolEvalResult EberNullOnAnyNullChild(DrgPul *pdrgpulChildren);

			// return Null if all children are Null
			static
			EBoolEvalResult EberNullOnAllNullChildren(DrgPul *pdrgpulChildren);

		public:
		
			// ctor
			explicit
			CScalar
				(
				IMemoryPool *pmp
				)
				: 
				COperator(pmp)
			{}

			// dtor
			virtual 
			~CScalar() {}

			// type of operator
			virtual
			BOOL FScalar() const
			{
				GPOS_ASSERT(!FPhysical() && !FLogical() && !FPattern());
				return true;
			}

			// create derived properties container
			virtual
			CDrvdProp *PdpCreate(IMemoryPool *pmp) const;

			// create required properties container
			virtual
			CReqdProp *PrpCreate(IMemoryPool *pmp) const;

			// return locally defined columns
			virtual
			CColRefSet *PcrsDefined
				(
				IMemoryPool *pmp,
				CExpressionHandle & // exprhdl
				)
			{
				// return an empty set of column refs
				return GPOS_NEW(pmp) CColRefSet(pmp);
			}

			// return columns containing set-returning function
			virtual
			CColRefSet *PcrsSetReturningFunction
				(
				IMemoryPool *pmp,
				CExpressionHandle & // exprhdl
				)
			{
				// return an empty set of column refs
				return GPOS_NEW(pmp) CColRefSet(pmp);
			}

			// return locally used columns
			virtual
			CColRefSet *PcrsUsed
				(
				IMemoryPool *pmp,
				CExpressionHandle & // exprhdl
				)
			{
				// return an empty set of column refs
				return GPOS_NEW(pmp) CColRefSet(pmp);
			}

			// derive partition consumer info
			virtual
			CPartInfo *PpartinfoDerive
				(
				IMemoryPool *pmp, 
				CExpressionHandle &exprhdl
				) 
				const
			{
				return PpartinfoDeriveCombineScalar(pmp, exprhdl);
			}

			// derive function properties
			virtual
			CFunctionProp *PfpDerive
				(
				IMemoryPool *pmp,
				CExpressionHandle &exprhdl
				)
				const
			{
				return PfpDeriveFromChildren
							(
							pmp,
							exprhdl,
							IMDFunction::EfsImmutable, // efsDefault
							IMDFunction::EfdaNoSQL, // efdaDefault
							false, // fHasVolatileFunctionScan
							false // fScan
							);
			}

			// derive subquery existence
			virtual
			BOOL FHasSubquery(CExpressionHandle &exprhdl);

			// derive non-scalar function existence
			virtual
			BOOL FHasNonScalarFunction(CExpressionHandle &exprhdl);

			// boolean expression evaluation
			virtual
			EBoolEvalResult Eber
				(
				DrgPul * // pdrgpulChildren
				)
				const
			{
				// by default, evaluation result is unknown
				return EberUnknown;
			}

			// perform boolean evaluation of the given expression tree
			static
			EBoolEvalResult EberEvaluate(IMemoryPool *pmp, CExpression *pexprScalar);

			// conversion function
			static
			CScalar *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(pop->FScalar());
				
				return reinterpret_cast<CScalar*>(pop);
			}
		
			// the type of the scalar expression
			virtual 
			IMDId *PmdidType() const = 0;

			// the type modifier of the scalar expression
			virtual
			INT ITypeModifier() const
			{
				return IDefaultTypeModifier;
			}
			
	}; // class CScalar

}


#endif // !GPOPT_CScalar_H

// EOF
