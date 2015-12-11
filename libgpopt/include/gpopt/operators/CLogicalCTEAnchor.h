//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalCTEAnchor.h
//
//	@doc:
//		Logical CTE anchor operator
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalCTEAnchor_H
#define GPOPT_CLogicalCTEAnchor_H

#include "gpos/base.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogical.h"

namespace gpopt
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CLogicalCTEAnchor
	//
	//	@doc:
	//		CTE anchor operator
	//
	//---------------------------------------------------------------------------
	class CLogicalCTEAnchor : public CLogical
	{
		private:

			// cte identifier
			ULONG m_ulId;

			// private copy ctor
			CLogicalCTEAnchor(const CLogicalCTEAnchor &);

		public:

			// ctor
			explicit
			CLogicalCTEAnchor(IMemoryPool *pmp);

			// ctor
			CLogicalCTEAnchor(IMemoryPool *pmp, ULONG ulId);

			// dtor
			virtual
			~CLogicalCTEAnchor()
			{}

			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopLogicalCTEAnchor;
			}

			virtual
			const CHAR *SzId() const
			{
				return "CLogicalCTEAnchor";
			}

			// cte identifier
			ULONG UlId() const
			{
				return m_ulId;
			}

			// operator specific hash function
			virtual
			ULONG UlHash() const;

			// match function
			virtual
			BOOL FMatch(COperator *pop) const;

			// sensitivity to order of inputs
			virtual
			BOOL FInputOrderSensitive() const
			{
				return false;
			}

			// return a copy of the operator with remapped columns
			virtual
			COperator *PopCopyWithRemappedColumns
						(
						IMemoryPool *, //pmp,
						HMUlCr *, //phmulcr,
						BOOL //fMustExist
						)
			{
				return PopCopyDefault();
			}

			//-------------------------------------------------------------------------------------
			// Derived Relational Properties
			//-------------------------------------------------------------------------------------

			// derive output columns
			virtual
			CColRefSet *PcrsDeriveOutput(IMemoryPool *pmp, CExpressionHandle &exprhdl);

			// dervive keys
			virtual
			CKeyCollection *PkcDeriveKeys(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

			// derive max card
			virtual
			CMaxCard Maxcard(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

			// derive constraint property
			virtual
			CPropConstraint *PpcDeriveConstraint
				(
				IMemoryPool *, //pmp,
				CExpressionHandle &exprhdl
				)
				const
			{
				return PpcDeriveConstraintPassThru(exprhdl, 0 /*ulChild*/);
			}

			// derive partition consumer info
			virtual
			CPartInfo *PpartinfoDerive(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

			// compute required stats columns of the n-th child
			virtual
			CColRefSet *PcrsStat
				(
				IMemoryPool *,// pmp
				CExpressionHandle &,// exprhdl
				CColRefSet *pcrsInput,
				ULONG // ulChildIndex
				)
				const
			{
				return PcrsStatsPassThru(pcrsInput);
			}

			// derive statistics
			virtual
			IStatistics *PstatsDerive
						(
						IMemoryPool *, //pmp,
						CExpressionHandle &exprhdl,
						DrgPstat * //pdrgpstatCtxt
						)
						const
			{
				return PstatsPassThruOuter(exprhdl);
			}

			// stat promise
			virtual
			EStatPromise Esp(CExpressionHandle &) const
			{
				return CLogical::EspHigh;
			}

			//-------------------------------------------------------------------------------------
			// Transformations
			//-------------------------------------------------------------------------------------

			// candidate set of xforms
			virtual
			CXformSet *PxfsCandidates(IMemoryPool *pmp) const;

			//-------------------------------------------------------------------------------------

			// conversion function
			static
			CLogicalCTEAnchor *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopLogicalCTEAnchor == pop->Eopid());

				return dynamic_cast<CLogicalCTEAnchor*>(pop);
			}

			// debug print
			virtual
			IOstream &OsPrint(IOstream &) const;

	}; // class CLogicalCTEAnchor

}

#endif // !GPOPT_CLogicalCTEAnchor_H

// EOF
