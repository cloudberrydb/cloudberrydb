//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp
//
//	@filename:
//		CPhysicalIndexScan.h
//
//	@doc:
//		Base class for physical index scan operators
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalIndexScan_H
#define GPOPT_CPhysicalIndexScan_H

#include "gpos/base.h"
#include "gpopt/operators/CPhysicalScan.h"
#include "gpopt/metadata/CIndexDescriptor.h"

namespace gpopt
{

	// fwd declarations
	class CTableDescriptor;
	class CIndexDescriptor;
	class CName;
	class CDistributionSpecHashed;

	//---------------------------------------------------------------------------
	//	@class:
	//		CPhysicalIndexScan
	//
	//	@doc:
	//		Base class for physical index scan operators
	//
	//---------------------------------------------------------------------------
	class CPhysicalIndexScan : public CPhysicalScan
	{

		private:

			// index descriptor
			CIndexDescriptor *m_pindexdesc;

			// origin operator id -- ULONG_MAX if operator was not generated via a transformation
			ULONG m_ulOriginOpId;

			// order
			COrderSpec *m_pos;

			// derive hashed distribution when index conditions have outer references
			CDistributionSpecHashed *PdshashedDeriveWithOuterRefs(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

			// private copy ctor
			CPhysicalIndexScan(const CPhysicalIndexScan&);

			// search the given array of predicates for an equality predicate that has one side equal to given expression
			static
			CExpression *PexprMatchEqualitySide
				(
				CExpression *pexprToMatch,
				DrgPexpr *pdrgpexpr // array of predicates to inspect
				);

		public:

			// ctors
			CPhysicalIndexScan
				(
				IMemoryPool *pmp,
				CIndexDescriptor *pindexdesc,
				CTableDescriptor *ptabdesc,
				ULONG ulOriginOpId,
				const CName *pnameAlias,
				DrgPcr *pdrgpcr,
				COrderSpec *pos
				);

			// dtor
			virtual
			~CPhysicalIndexScan();


			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopPhysicalIndexScan;
			}

			// operator name
			virtual
			const CHAR *SzId() const
			{
				return "CPhysicalIndexScan";
			}

			// table alias name
			const CName &NameAlias() const
			{
				return *m_pnameAlias;
			}

			// origin operator id -- ULONG_MAX if operator was not generated via a transformation
			ULONG UlOriginOpId() const
			{
				return m_ulOriginOpId;
			}

			// derive distribution
			virtual
			CDistributionSpec *PdsDerive(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

			// operator specific hash function
			virtual
			ULONG UlHash() const;

			// match function
			BOOL FMatch(COperator *pop) const;

			// index descriptor
			CIndexDescriptor *Pindexdesc() const
			{
				return m_pindexdesc;
			}

			// sensitivity to order of inputs
			virtual
			BOOL FInputOrderSensitive() const
			{
				return true;
			}

			//-------------------------------------------------------------------------------------
			// Derived Plan Properties
			//-------------------------------------------------------------------------------------

			// derive sort order
			virtual
			COrderSpec *PosDerive
							(
							IMemoryPool *,//pmp
							CExpressionHandle &//exprhdl
							)
							const
			{
				m_pos->AddRef();
				return m_pos;
			}

			// derive partition index map
			virtual
			CPartIndexMap *PpimDerive
				(
				IMemoryPool *pmp,
				CExpressionHandle &, // exprhdl
				CDrvdPropCtxt * //pdpctxt
				)
				const
			{
				return GPOS_NEW(pmp) CPartIndexMap(pmp);
			}
			
			//-------------------------------------------------------------------------------------
			// Enforced Properties
			//-------------------------------------------------------------------------------------

			// return order property enforcing type for this operator
			virtual
			CEnfdProp::EPropEnforcingType EpetOrder(CExpressionHandle &exprhdl, const CEnfdOrder *peo) const;

			// conversion function
			static
			CPhysicalIndexScan *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopPhysicalIndexScan == pop->Eopid());

				return dynamic_cast<CPhysicalIndexScan*>(pop);
			}

			// statistics derivation during costing
			virtual
			IStatistics *PstatsDerive
				(
				IMemoryPool *, // pmp
				CExpressionHandle &, // exprhdl
				CReqdPropPlan *, // prpplan
				DrgPstat * //pdrgpstatCtxt
				)
				const
			{
				GPOS_ASSERT(!"stats derivation during costing for index scan is invalid");

				return NULL;
			}

			// debug print
			virtual
			IOstream &OsPrint(IOstream &) const;

	}; // class CPhysicalIndexScan

}

#endif // !GPOPT_CPhysicalIndexScan_H

// EOF
