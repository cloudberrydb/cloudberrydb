//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalTVF.h
//
//	@doc:
//		Table-valued function
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalTVF_H
#define GPOPT_CLogicalTVF_H

#include "gpos/base.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/operators/CLogical.h"

namespace gpopt
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CLogicalTVF
	//
	//	@doc:
	//		Table-valued function
	//
	//---------------------------------------------------------------------------
	class CLogicalTVF : public CLogical
	{

		private:
		
			// function mdid
			IMDId *m_pmdidFunc;
			
			// return type
			IMDId *m_pmdidRetType;

			// function name
			CWStringConst *m_pstr;
			
			// array of column descriptors: the schema of the function result
			DrgPcoldesc *m_pdrgpcoldesc;
				
			// output columns
			DrgPcr *m_pdrgpcrOutput;
			
			// function stability
			IMDFunction::EFuncStbl m_efs;

			// function data access
			IMDFunction::EFuncDataAcc m_efda;

			// does this function return a set of rows
			BOOL m_fReturnsSet;

			// private copy ctor
			CLogicalTVF(const CLogicalTVF &);
			
		public:
		
			// ctors
			explicit
			CLogicalTVF(IMemoryPool *pmp);

			CLogicalTVF
				(
				IMemoryPool *pmp,
				IMDId *pmdidFunc,
				IMDId *pmdidRetType,
				CWStringConst *pstr,
				DrgPcoldesc *pdrgpcoldesc
				);

			CLogicalTVF
				(
				IMemoryPool *pmp,
				IMDId *pmdidFunc,
				IMDId *pmdidRetType,
				CWStringConst *pstr,
				DrgPcoldesc *pdrgpcoldesc,
				DrgPcr *pdrgpcrOutput
				);

			// dtor
			virtual 
			~CLogicalTVF();

			// ident accessors
			virtual 
			EOperatorId Eopid() const
			{
				return EopLogicalTVF;
			}
			
			// return a string for operator name
			virtual 
			const CHAR *SzId() const
			{
				return "CLogicalTVF";
			}
			
			// function mdid
			IMDId *PmdidFunc() const
			{
				return m_pmdidFunc;
			}
			
			// return type
			IMDId *PmdidRetType() const
			{
				return m_pmdidRetType;
			}

			// function name
			const CWStringConst *Pstr() const
			{
				return m_pstr;
			}

			// col descr accessor
			DrgPcoldesc *Pdrgpcoldesc() const
			{
				return m_pdrgpcoldesc;
			}
			
			// accessors
			DrgPcr *PdrgpcrOutput() const
			{
				return m_pdrgpcrOutput;
			}

			// sensitivity to order of inputs
			BOOL FInputOrderSensitive() const;

			// operator specific hash function
			virtual
			ULONG UlHash() const;

			// match function
			virtual
			BOOL FMatch(COperator *pop) const;
			
			// return a copy of the operator with remapped columns
			virtual
			COperator *PopCopyWithRemappedColumns(IMemoryPool *pmp, HMUlCr *phmulcr, BOOL fMustExist);

			//-------------------------------------------------------------------------------------
			// Derived Relational Properties
			//-------------------------------------------------------------------------------------

			// derive output columns
			virtual
			CColRefSet *PcrsDeriveOutput(IMemoryPool *, CExpressionHandle &);

			// derive partition consumer info
			virtual
			CPartInfo *PpartinfoDerive
				(
				IMemoryPool *pmp,
				CExpressionHandle & //exprhdl
				) 
				const
			{
				return GPOS_NEW(pmp) CPartInfo(pmp);
			}
			
			// derive constraint property
			virtual
			CPropConstraint *PpcDeriveConstraint
				(
				IMemoryPool *pmp,
				CExpressionHandle & //exprhdl
				)
				const
			{
				return GPOS_NEW(pmp) CPropConstraint(pmp, GPOS_NEW(pmp) DrgPcrs(pmp), NULL /*pcnstr*/);
			}

			// derive function properties
			virtual
			CFunctionProp *PfpDerive(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

			// derive max card
			virtual
			CMaxCard Maxcard(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

			//-------------------------------------------------------------------------------------
			// Required Relational Properties
			//-------------------------------------------------------------------------------------

			// compute required stat columns of the n-th child
			virtual
			CColRefSet *PcrsStat
				(
				IMemoryPool *,// pmp
				CExpressionHandle &,// exprhdl
				CColRefSet *,// pcrsInput
				ULONG // ulChildIndex
				)
				const
			{
				return NULL;
			}

			//-------------------------------------------------------------------------------------
			// Transformations
			//-------------------------------------------------------------------------------------

			// candidate set of xforms
			virtual
			CXformSet *PxfsCandidates(IMemoryPool *pmp) const;

			// stat promise
			virtual
			EStatPromise Esp(CExpressionHandle &) const
			{
				return CLogical::EspLow;
			}

			// derive statistics
			virtual
			IStatistics *PstatsDerive(IMemoryPool *pmp, CExpressionHandle &exprhdl, DrgPstat *pdrgpstatCtxt) const;

			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------

			// conversion function
			static
			CLogicalTVF *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopLogicalTVF == pop->Eopid());
				
				return dynamic_cast<CLogicalTVF*>(pop);
			}
			

			// debug print
			virtual 
			IOstream &OsPrint(IOstream &) const;

	}; // class CLogicalTVF

}


#endif // !GPOPT_CLogicalTVF_H

// EOF
