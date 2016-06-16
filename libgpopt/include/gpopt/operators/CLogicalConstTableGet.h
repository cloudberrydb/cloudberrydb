//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CLogicalConstTableGet.h
//
//	@doc:
//		Constant table accessor
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalConstTableGet_H
#define GPOPT_CLogicalConstTableGet_H

#include "gpos/base.h"
#include "gpopt/operators/CLogical.h"

namespace gpopt
{
	// dynamic array of datum arrays -- array owns elements
	typedef CDynamicPtrArray<DrgPdatum, CleanupRelease> DrgPdrgPdatum;

	//---------------------------------------------------------------------------
	//	@class:
	//		CLogicalConstTableGet
	//
	//	@doc:
	//		Constant table accessor
	//
	//---------------------------------------------------------------------------
	class CLogicalConstTableGet : public CLogical
	{

		private:
			// array of column descriptors: the schema of the const table
			DrgPcoldesc *m_pdrgpcoldesc;
		
			// array of datum arrays
			DrgPdrgPdatum *m_pdrgpdrgpdatum;
			
			// output columns
			DrgPcr *m_pdrgpcrOutput;
			
			// private copy ctor
			CLogicalConstTableGet(const CLogicalConstTableGet &);
			
			// construct column descriptors from column references
			DrgPcoldesc *PdrgpcoldescMapping(IMemoryPool *pmp, DrgPcr *pdrgpcr)	const;

		public:
		
			// ctors
			explicit
			CLogicalConstTableGet(IMemoryPool *pmp);

			CLogicalConstTableGet
				(
				IMemoryPool *pmp,
				DrgPcoldesc *pdrgpcoldesc,
				DrgPdrgPdatum *pdrgpdrgpdatum
				);

			CLogicalConstTableGet
				(
				IMemoryPool *pmp,
				DrgPcr *pdrgpcrOutput,
				DrgPdrgPdatum *pdrgpdrgpdatum
				);

			// dtor
			virtual 
			~CLogicalConstTableGet();

			// ident accessors
			virtual 
			EOperatorId Eopid() const
			{
				return EopLogicalConstTableGet;
			}
			
			// return a string for operator name
			virtual 
			const CHAR *SzId() const
			{
				return "CLogicalConstTableGet";
			}
			
			// col descr accessor
			DrgPcoldesc *Pdrgpcoldesc() const
			{
				return m_pdrgpcoldesc;
			}
			
			// const table values accessor
			DrgPdrgPdatum *Pdrgpdrgpdatum () const
			{
				return m_pdrgpdrgpdatum;
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
				
			// derive max card
			virtual
			CMaxCard Maxcard(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

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
				CExpressionHandle & // exprhdl
				)
				const
			{
				// TODO:  - Jan 11, 2013; compute constraints based on the
				// datum values in this CTG
				return GPOS_NEW(pmp) CPropConstraint(pmp, GPOS_NEW(pmp) DrgPcrs(pmp), NULL /*pcnstr*/);
			}

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
				GPOS_ASSERT(!"CLogicalConstTableGet has no children");
				return NULL;
			}

			// derive statistics
			virtual
			IStatistics *PstatsDerive
						(
						IMemoryPool *pmp,
						CExpressionHandle &exprhdl,
						DrgPstat *pdrgpstatCtxt
						)
						const;

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

			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------

			// conversion function
			static
			CLogicalConstTableGet *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopLogicalConstTableGet == pop->Eopid());
				
				return dynamic_cast<CLogicalConstTableGet*>(pop);
			}
			

			// debug print
			virtual 
			IOstream &OsPrint(IOstream &) const;

	}; // class CLogicalConstTableGet

}


#endif // !GPOPT_CLogicalConstTableGet_H

// EOF
