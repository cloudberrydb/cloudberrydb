//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalInsert.h
//
//	@doc:
//		Logical Insert operator
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalInsert_H
#define GPOPT_CLogicalInsert_H

#include "gpos/base.h"
#include "gpopt/operators/CLogical.h"

namespace gpopt
{

	// fwd declarations
	class CTableDescriptor;

	//---------------------------------------------------------------------------
	//	@class:
	//		CLogicalInsert
	//
	//	@doc:
	//		Logical Insert operator
	//
	//---------------------------------------------------------------------------
	class CLogicalInsert : public CLogical
	{

		private:

			// table descriptor
			CTableDescriptor *m_ptabdesc;

			// source columns
			DrgPcr *m_pdrgpcrSource;

			// private copy ctor
			CLogicalInsert(const CLogicalInsert &);

		public:

			// ctor
			explicit
			CLogicalInsert(IMemoryPool *pmp);

			// ctor
			CLogicalInsert(IMemoryPool *pmp, CTableDescriptor *ptabdesc, DrgPcr *pdrgpcr);

			// dtor
			virtual
			~CLogicalInsert();

			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopLogicalInsert;
			}

			// return a string for operator name
			virtual
			const CHAR *SzId() const
			{
				return "CLogicalInsert";
			}

			// source columns
			DrgPcr *PdrgpcrSource() const
			{
				return m_pdrgpcrSource;
			}

			// return table's descriptor
			CTableDescriptor *Ptabdesc() const
			{
				return m_ptabdesc;
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
			COperator *PopCopyWithRemappedColumns(IMemoryPool *pmp, HMUlCr *phmulcr, BOOL fMustExist);

			//-------------------------------------------------------------------------------------
			// Derived Relational Properties
			//-------------------------------------------------------------------------------------

			// derive output columns
			virtual
			CColRefSet *PcrsDeriveOutput(IMemoryPool *pmp, CExpressionHandle &exprhdl);


			// derive constraint property
			virtual
			CPropConstraint *PpcDeriveConstraint
				(
				IMemoryPool *, // pmp
				CExpressionHandle &exprhdl
				)
				const
			{
				return CLogical::PpcDeriveConstraintPassThru(exprhdl, 0 /*ulChild*/);
			}

			// derive max card
			virtual
			CMaxCard Maxcard(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

			// derive partition consumer info
			virtual
			CPartInfo *PpartinfoDerive
				(
				IMemoryPool *, // pmp,
				CExpressionHandle &exprhdl
				)
				const
			{
				return PpartinfoPassThruOuter(exprhdl);
			}

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

			//-------------------------------------------------------------------------------------
			// Transformations
			//-------------------------------------------------------------------------------------

			// candidate set of xforms
			virtual
			CXformSet *PxfsCandidates(IMemoryPool *pmp) const;

			// derive key collections
			virtual
			CKeyCollection *PkcDeriveKeys(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

			// derive statistics
			virtual
			IStatistics *PstatsDerive
						(
						IMemoryPool *pmp,
						CExpressionHandle &exprhdl,
						DrgPstat *pdrgpstatCtxt
						)
						const;

			// stat promise
			virtual
			EStatPromise Esp(CExpressionHandle &) const
			{
				return CLogical::EspHigh;
			}

			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------

			// conversion function
			static
			CLogicalInsert *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopLogicalInsert == pop->Eopid());

				return dynamic_cast<CLogicalInsert*>(pop);
			}

			// debug print
			virtual
			IOstream &OsPrint(IOstream &) const;

	}; // class CLogicalInsert
}

#endif // !GPOPT_CLogicalInsert_H

// EOF
