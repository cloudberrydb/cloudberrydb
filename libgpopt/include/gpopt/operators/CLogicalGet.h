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
			CColRefArray *m_pdrgpcrOutput;
			
			// partition keys
			CColRef2dArray *m_pdrgpdrgpcrPart;

			// distribution columns (empty for master only tables)
			CColRefSet *m_pcrsDist;

			void
			CreatePartCols(IMemoryPool *mp, const ULongPtrArray *pdrgpulPart);
			
			// private copy ctor
			CLogicalGet(const CLogicalGet &);

		public:
		
			// ctors
			explicit
			CLogicalGet(IMemoryPool *mp);

			CLogicalGet
				(
				IMemoryPool *mp,
				const CName *pnameAlias,
				CTableDescriptor *ptabdesc
				);

			CLogicalGet
				(
				IMemoryPool *mp,
				const CName *pnameAlias,
				CTableDescriptor *ptabdesc,
				CColRefArray *pdrgpcrOutput
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
			CColRefArray *PdrgpcrOutput() const
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
			CColRef2dArray *
			PdrgpdrgpcrPartColumns() const
			{
				return m_pdrgpdrgpcrPart;
			}
			
			// operator specific hash function
			virtual
			ULONG HashValue() const;

			// match function
			BOOL Matches(COperator *pop) const;

			// sensitivity to order of inputs
			BOOL FInputOrderSensitive() const;

			// return a copy of the operator with remapped columns
			virtual
			COperator *PopCopyWithRemappedColumns(IMemoryPool *mp, UlongToColRefMap *colref_mapping, BOOL must_exist);

			//-------------------------------------------------------------------------------------
			// Derived Relational Properties
			//-------------------------------------------------------------------------------------

			// derive output columns
			virtual
			CColRefSet *PcrsDeriveOutput(IMemoryPool *mp, CExpressionHandle &exprhdl);

			// derive not nullable output columns
			virtual
			CColRefSet *PcrsDeriveNotNull(IMemoryPool *mp, CExpressionHandle &exprhdl) const;

			// derive partition consumer info
			virtual
			CPartInfo *PpartinfoDerive
				(
				IMemoryPool *mp,
				CExpressionHandle & // exprhdl
				) 
				const
			{
				return GPOS_NEW(mp) CPartInfo(mp);
			}
			
			// derive constraint property
			virtual
			CPropConstraint *PpcDeriveConstraint
				(
				IMemoryPool *mp,
				CExpressionHandle & // exprhdl
				)
				const
			{
				return PpcDeriveConstraintFromTable(mp, m_ptabdesc, m_pdrgpcrOutput);
			}

			// derive join depth
			virtual
			ULONG JoinDepth
				(
				IMemoryPool *, // mp
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
				IMemoryPool *, // mp,
				CExpressionHandle &, // exprhdl
				CColRefSet *, // pcrsInput
				ULONG // child_index
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
