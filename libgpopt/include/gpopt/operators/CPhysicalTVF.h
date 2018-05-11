//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalTVF.h
//
//	@doc:
//		Physical Table-valued function
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalTVF_H
#define GPOPT_CPhysicalTVF_H

#include "gpos/base.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/CPhysical.h"

namespace gpopt
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CPhysicalTVF
	//
	//	@doc:
	//		Physical Table-valued function
	//
	//---------------------------------------------------------------------------
	class CPhysicalTVF : public CPhysical
	{
		private:

			// function mdid
			IMDId *m_pmdidFunc;

			// return type
			IMDId *m_pmdidRetType;

			// function name
			CWStringConst *m_pstr;

			// MD cache info
			const IMDFunction *m_pmdfunc;

			// array of column descriptors: the schema of the function result
			DrgPcoldesc *m_pdrgpcoldesc;

			// output columns
			CColRefSet *m_pcrsOutput;

			// private copy ctor
			CPhysicalTVF(const CPhysicalTVF &);

		public:

			// ctor
			CPhysicalTVF
				(
				IMemoryPool *pmp,
				IMDId *pmdidFunc,
				IMDId *pmdidRetType,
				CWStringConst *pstr,
				DrgPcoldesc *pdrgpcoldesc,
				CColRefSet *pcrsOutput
				);

			// dtor
			virtual
			~CPhysicalTVF();

			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopPhysicalTVF;
			}

			// return a string for operator name
			virtual
			const CHAR *SzId() const
			{
				return "CPhysicalTVF";
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
			CColRefSet *PcrsOutput() const
			{
				return m_pcrsOutput;
			}

			// sensitivity to order of inputs
			BOOL FInputOrderSensitive() const;

			// match function
			virtual
			BOOL FMatch(COperator *pop) const;

			//-------------------------------------------------------------------------------------
			// Required Plan Properties
			//-------------------------------------------------------------------------------------

			// compute required output columns of the n-th child
			virtual
			CColRefSet *PcrsRequired
				(
				IMemoryPool *pmp,
				CExpressionHandle &exprhdl,
				CColRefSet *pcrsRequired,
				ULONG ulChildIndex,
				DrgPdp *pdrgpdpCtxt,
				ULONG ulOptReq
				);

			// compute required ctes of the n-th child
			virtual
			CCTEReq *PcteRequired
				(
				IMemoryPool *pmp,
				CExpressionHandle &exprhdl,
				CCTEReq *pcter,
				ULONG ulChildIndex,
				DrgPdp *pdrgpdpCtxt,
				ULONG ulOptReq
				)
				const;

			// compute required sort order of the n-th child
			virtual
			COrderSpec *PosRequired
				(
				IMemoryPool *pmp,
				CExpressionHandle &exprhdl,
				COrderSpec *posRequired,
				ULONG ulChildIndex,
				DrgPdp *pdrgpdpCtxt,
				ULONG ulOptReq
				)
				const;

			// compute required distribution of the n-th child
			virtual
			CDistributionSpec *PdsRequired
				(
				IMemoryPool *pmp,
				CExpressionHandle &exprhdl,
				CDistributionSpec *pdsRequired,
				ULONG ulChildIndex,
				DrgPdp *pdrgpdpCtxt,
				ULONG ulOptReq
				)
				const;

			// compute required rewindability of the n-th child
			virtual
			CRewindabilitySpec *PrsRequired
				(
				IMemoryPool *pmp,
				CExpressionHandle &exprhdl,
				CRewindabilitySpec *prsRequired,
				ULONG ulChildIndex,
				DrgPdp *pdrgpdpCtxt,
				ULONG ulOptReq
				)
				const;
			
			// compute required partition propagation of the n-th child
			virtual
			CPartitionPropagationSpec *PppsRequired
				(
				IMemoryPool *, //pmp,
				CExpressionHandle &, //exprhdl,
				CPartitionPropagationSpec *, //pppsRequired,
				ULONG , //ulChildIndex,
				DrgPdp *, //pdrgpdpCtxt,
				ULONG //ulOptReq
				)
			{
				GPOS_ASSERT(!"CPhysicalTVF has no relational children");
				return NULL;
			}

			// check if required columns are included in output columns
			virtual
			BOOL FProvidesReqdCols(CExpressionHandle &exprhdl, CColRefSet *pcrsRequired, ULONG ulOptReq) const;

			//-------------------------------------------------------------------------------------
			// Derived Plan Properties
			//-------------------------------------------------------------------------------------

			// derive sort order
			virtual
			COrderSpec *PosDerive(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

			// derive distribution
			virtual
			CDistributionSpec *PdsDerive(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

			// derive rewindability
			virtual
			CRewindabilitySpec *PrsDerive(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

			// derive cte map
			virtual
			CCTEMap *PcmDerive(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

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
			
			// derive partition filter map
			virtual
			CPartFilterMap *PpfmDerive
				(
				IMemoryPool *pmp,
				CExpressionHandle & // exprhdl
				)
				const
			{
				// return empty part filter map
				return GPOS_NEW(pmp) CPartFilterMap(pmp);
			}

			//-------------------------------------------------------------------------------------
			// Enforced Properties
			//-------------------------------------------------------------------------------------

			// return order property enforcing type for this operator
			virtual
			CEnfdProp::EPropEnforcingType EpetOrder
				(
				CExpressionHandle &exprhdl,
				const CEnfdOrder *peo
				)
				const;


			// return rewindability property enforcing type for this operator
			virtual
			CEnfdProp::EPropEnforcingType EpetRewindability
				(
				CExpressionHandle &exprhdl,
				const CEnfdRewindability *per
				)
				const;
			
			// return partition propagation property enforcing type for this operator
			virtual 
			CEnfdProp::EPropEnforcingType EpetPartitionPropagation
				(
				CExpressionHandle &, // exprhdl,
				const CEnfdPartitionPropagation * // pepp
				) 
				const
			{
				return CEnfdProp::EpetRequired;
			}

			// return true if operator passes through stats obtained from children,
			// this is used when computing stats during costing
			virtual
			BOOL FPassThruStats() const
			{
				return false;
			}

			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------

			// conversion function
			static
			CPhysicalTVF *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopPhysicalTVF == pop->Eopid());

				return dynamic_cast<CPhysicalTVF*>(pop);
			}

	}; // class CPhysicalTVF

}

#endif // !GPOPT_CPhysicalTVF_H

// EOF
