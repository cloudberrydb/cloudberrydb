//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CLogicalGet.h
//
//	@doc:
//		Basic table accessor
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalGet_H
#define GPOPT_CLogicalGet_H

#include "gpos/base.h"
#include "gpopt/operators/CLogical.h"

namespace gpopt
{
	
	// fwd declarations
	class CTableDescriptor;
	class CName;
	class CColRefSet;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CLogicalGet
	//
	//	@doc:
	//		Basic table accessor
	//
	//---------------------------------------------------------------------------
	class CLogicalGet : public CLogical
	{

		private:

			// alias for table
			const CName *m_pnameAlias;

			// table descriptor
			CTableDescriptor *m_ptabdesc;
			
			// output columns
			DrgPcr *m_pdrgpcrOutput;
			
			// partition keys
			DrgDrgPcr *m_pdrgpdrgpcrPart;

			// distribution columns (empty for master only tables)
			CColRefSet *m_pcrsDist;

			void
			CreatePartCols(IMemoryPool *pmp, const DrgPul *pdrgpulPart);
			
			// private copy ctor
			CLogicalGet(const CLogicalGet &);

		public:
		
			// ctors
			explicit
			CLogicalGet(IMemoryPool *pmp);

			CLogicalGet
				(
				IMemoryPool *pmp,
				const CName *pnameAlias,
				CTableDescriptor *ptabdesc
				);

			CLogicalGet
				(
				IMemoryPool *pmp,
				const CName *pnameAlias,
				CTableDescriptor *ptabdesc,
				DrgPcr *pdrgpcrOutput
				);

			// dtor
			virtual 
			~CLogicalGet();

			// ident accessors
			virtual 
			EOperatorId Eopid() const
			{
				return EopLogicalGet;
			}
			
			// distribution columns
			virtual
			const CColRefSet *PcrsDist() const
			{
				return m_pcrsDist;
			}

			// return a string for operator name
			virtual 
			const CHAR *SzId() const
			{
				return "CLogicalGet";
			}

			// accessors
			DrgPcr *PdrgpcrOutput() const
			{
				return m_pdrgpcrOutput;
			}
			
			// return table's name
			const CName &Name() const
			{
				return *m_pnameAlias;
			}
			
			// return table's descriptor
			CTableDescriptor *Ptabdesc() const
			{
				return m_ptabdesc;
			}
			
			// partition columns
			DrgDrgPcr *
			PdrgpdrgpcrPartColumns() const
			{
				return m_pdrgpdrgpcrPart;
			}
			
			// operator specific hash function
			virtual
			ULONG UlHash() const;

			// match function
			BOOL FMatch(COperator *pop) const;

			// sensitivity to order of inputs
			BOOL FInputOrderSensitive() const;

			// return a copy of the operator with remapped columns
			virtual
			COperator *PopCopyWithRemappedColumns(IMemoryPool *pmp, HMUlCr *phmulcr, BOOL fMustExist);

			//-------------------------------------------------------------------------------------
			// Derived Relational Properties
			//-------------------------------------------------------------------------------------

			// derive output columns
			virtual
			CColRefSet *PcrsDeriveOutput(IMemoryPool *pmp, CExpressionHandle &exprhdl);

			// derive not nullable output columns
			virtual
			CColRefSet *PcrsDeriveNotNull(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

			// derive partition consumer info
			virtual
			CPartInfo *PpartinfoDerive
				(
				IMemoryPool *pmp,
				CExpressionHandle & // exprhdl
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
				return PpcDeriveConstraintFromTable(pmp, m_ptabdesc, m_pdrgpcrOutput);
			}

			// derive join depth
			virtual
			ULONG UlJoinDepth
				(
				IMemoryPool *, // pmp
				CExpressionHandle & // exprhdl
				)
				const
			{
				return 1;
			}

			//-------------------------------------------------------------------------------------
			// Required Relational Properties
			//-------------------------------------------------------------------------------------

			// compute required stat columns of the n-th child
			virtual
			CColRefSet *PcrsStat
				(
				IMemoryPool *, // pmp,
				CExpressionHandle &, // exprhdl
				CColRefSet *, // pcrsInput
				ULONG // ulChildIndex
				)
				const
			{
				GPOS_ASSERT(!"CLogicalGet has no children");
				return NULL;
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
			CLogicalGet *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopLogicalGet == pop->Eopid() ||
							EopLogicalExternalGet == pop->Eopid());
				
				return dynamic_cast<CLogicalGet*>(pop);
			}
			
			// debug print
			virtual 
			IOstream &OsPrint(IOstream &) const;

	}; // class CLogicalGet

}


#endif // !GPOPT_CLogicalGet_H

// EOF
