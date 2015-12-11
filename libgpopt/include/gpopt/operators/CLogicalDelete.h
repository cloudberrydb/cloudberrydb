//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalDelete.h
//
//	@doc:
//		Logical Delete operator
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalDelete_H
#define GPOPT_CLogicalDelete_H

#include "gpos/base.h"
#include "gpopt/operators/CLogical.h"

namespace gpopt
{

	// fwd declarations
	class CTableDescriptor;

	//---------------------------------------------------------------------------
	//	@class:
	//		CLogicalDelete
	//
	//	@doc:
	//		Logical Delete operator
	//
	//---------------------------------------------------------------------------
	class CLogicalDelete : public CLogical
	{

		private:

			// table descriptor
			CTableDescriptor *m_ptabdesc;

			// columns to delete
			DrgPcr *m_pdrgpcr;

			// ctid column
			CColRef *m_pcrCtid;

			// segmentId column
			CColRef *m_pcrSegmentId;

			// private copy ctor
			CLogicalDelete(const CLogicalDelete &);

		public:

			// ctor
			explicit
			CLogicalDelete(IMemoryPool *pmp);

			// ctor
			CLogicalDelete
				(
				IMemoryPool *pmp,
				CTableDescriptor *ptabdesc,
				DrgPcr *pdrgpcr,
				CColRef *pcrCtid,
				CColRef *pcrSegmentId
				);

			// dtor
			virtual
			~CLogicalDelete();

			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopLogicalDelete;
			}

			// return a string for operator name
			virtual
			const CHAR *SzId() const
			{
				return "CLogicalDelete";
			}

			// columns to delete
			DrgPcr *Pdrgpcr() const
			{
				return m_pdrgpcr;
			}

			// ctid column
			CColRef *PcrCtid() const
			{
				return m_pcrCtid;
			}

			// segmentId column
			CColRef *PcrSegmentId() const
			{
				return m_pcrSegmentId;
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
			CLogicalDelete *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopLogicalDelete == pop->Eopid());

				return dynamic_cast<CLogicalDelete*>(pop);
			}

			// debug print
			virtual
			IOstream &OsPrint(IOstream &) const;

	}; // class CLogicalDelete
}

#endif // !GPOPT_CLogicalDelete_H

// EOF
