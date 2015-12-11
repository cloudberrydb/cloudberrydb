//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalRowTrigger.h
//
//	@doc:
//		Logical row-level trigger operator
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalRowTrigger_H
#define GPOPT_CLogicalRowTrigger_H

#include "gpos/base.h"
#include "gpopt/operators/CLogical.h"

namespace gpopt
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CLogicalRowTrigger
	//
	//	@doc:
	//		Logical row-level trigger operator
	//
	//---------------------------------------------------------------------------
	class CLogicalRowTrigger : public CLogical
	{

		private:

			// relation id on which triggers are to be executed
			IMDId *m_pmdidRel;

			// trigger type
			INT m_iType;

			// old columns
			DrgPcr *m_pdrgpcrOld;

			// new columns
			DrgPcr *m_pdrgpcrNew;

			// stability
			IMDFunction::EFuncStbl m_efs;

			// data access
			IMDFunction::EFuncDataAcc m_efda;

			// private copy ctor
			CLogicalRowTrigger(const CLogicalRowTrigger &);

			// initialize function properties
			void InitFunctionProperties();

			// return the type of a given trigger as an integer
			INT ITriggerType(const IMDTrigger *pmdtrigger) const;

		public:

			// ctor
			explicit
			CLogicalRowTrigger(IMemoryPool *pmp);

			// ctor
			CLogicalRowTrigger
				(
				IMemoryPool *pmp,
				IMDId *pmdidRel,
				INT iType,
				DrgPcr *pdrgpcrOld,
				DrgPcr *pdrgpcrNew
				);

			// dtor
			virtual
			~CLogicalRowTrigger();

			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopLogicalRowTrigger;
			}

			// return a string for operator name
			virtual
			const CHAR *SzId() const
			{
				return "CLogicalRowTrigger";
			}

			// relation id
			IMDId *PmdidRel() const
			{
				return m_pmdidRel;
			}

			// trigger type
			INT IType() const
			{
				return m_iType;
			}

			// old columns
			DrgPcr *PdrgpcrOld() const
			{
				return m_pdrgpcrOld;
			}

			// new columns
			DrgPcr *PdrgpcrNew() const
			{
				return m_pdrgpcrNew;
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

			// derive function properties
			virtual
			CFunctionProp *PfpDerive(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

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
			CLogicalRowTrigger *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopLogicalRowTrigger == pop->Eopid());

				return dynamic_cast<CLogicalRowTrigger*>(pop);
			}

			// debug print
			virtual
			IOstream &OsPrint(IOstream &) const;

	}; // class CLogicalRowTrigger
}

#endif // !GPOPT_CLogicalRowTrigger_H

// EOF
