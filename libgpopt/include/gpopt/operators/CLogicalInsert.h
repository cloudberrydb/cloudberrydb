//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalInsert.h
//
//	@doc:
//		Logical Insert operator
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
			CColRefArray *m_pdrgpcrSource;

			// private copy ctor
			CLogicalInsert(const CLogicalInsert &);

		public:

			// ctor
			explicit
			CLogicalInsert(IMemoryPool *mp);

			// ctor
			CLogicalInsert(IMemoryPool *mp, CTableDescriptor *ptabdesc, CColRefArray *colref_array);

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
			CColRefArray *PdrgpcrSource() const
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
			ULONG HashValue() const;

			// match function
			virtual
			BOOL Matches(COperator *pop) const;

			// sensitivity to order of inputs
			virtual
			BOOL FInputOrderSensitive() const
			{
				return false;
			}

			// return a copy of the operator with remapped columns
			virtual
			COperator *PopCopyWithRemappedColumns(IMemoryPool *mp, UlongToColRefMap *colref_mapping, BOOL must_exist);

			//-------------------------------------------------------------------------------------
			// Derived Relational Properties
			//-------------------------------------------------------------------------------------

			// derive output columns
			virtual
			CColRefSet *PcrsDeriveOutput(IMemoryPool *mp, CExpressionHandle &exprhdl);


			// derive constraint property
			virtual
			CPropConstraint *PpcDeriveConstraint
				(
				IMemoryPool *, // mp
				CExpressionHandle &exprhdl
				)
				const
			{
				return CLogical::PpcDeriveConstraintPassThru(exprhdl, 0 /*ulChild*/);
			}

			// derive max card
			virtual
			CMaxCard Maxcard(IMemoryPool *mp, CExpressionHandle &exprhdl) const;

			// derive partition consumer info
			virtual
			CPartInfo *PpartinfoDerive
				(
				IMemoryPool *, // mp,
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
				IMemoryPool *,// mp
				CExpressionHandle &,// exprhdl
				CColRefSet *pcrsInput,
				ULONG // child_index
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
			CXformSet *PxfsCandidates(IMemoryPool *mp) const;

			// derive key collections
			virtual
			CKeyCollection *PkcDeriveKeys(IMemoryPool *mp, CExpressionHandle &exprhdl) const;

			// derive statistics
			virtual
			IStatistics *PstatsDerive
						(
						IMemoryPool *mp,
						CExpressionHandle &exprhdl,
						IStatisticsArray *stats_ctxt
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
