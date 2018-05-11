//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalSequenceProject.h
//
//	@doc:
//		Physical Sequence Project operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalSequenceProject_H
#define GPOPT_CPhysicalSequenceProject_H

#include "gpos/base.h"
#include "gpopt/base/CWindowFrame.h"
#include "gpopt/operators/CPhysical.h"

namespace gpopt
{

	// fwd declarations
	class CDistributionSpec;

	//---------------------------------------------------------------------------
	//	@class:
	//		CPhysicalSequenceProject
	//
	//	@doc:
	//		Physical Sequence Project operator
	//
	//---------------------------------------------------------------------------
	class CPhysicalSequenceProject : public CPhysical
	{

		private:

			// partition by keys
			CDistributionSpec *m_pds;

			// order specs of child window functions
			DrgPos *m_pdrgpos;

			// frames of child window functions
			DrgPwf *m_pdrgpwf;

			// order spec to request from child
			COrderSpec *m_pos;

			// required columns in order/frame specs
			CColRefSet *m_pcrsRequiredLocal;

			// create local order spec
			void CreateOrderSpec(IMemoryPool *pmp);

			// compute local required columns
			void ComputeRequiredLocalColumns(IMemoryPool *pmp);

			// private copy ctor
			CPhysicalSequenceProject(const CPhysicalSequenceProject &);

		public:

			// ctor
			CPhysicalSequenceProject
				(
				IMemoryPool *pmp,
				CDistributionSpec *pds,
				DrgPos *pdrgpos,
				DrgPwf *pdrgpwf
				);

			// dtor
			virtual
			~CPhysicalSequenceProject();

			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopPhysicalSequenceProject;
			}

			// operator name
			virtual
			const CHAR *SzId() const
			{
				return "CPhysicalSequenceProject";
			}

			// partition by keys
			CDistributionSpec *Pds() const
			{
				return m_pds;
			}

			// order by keys
			DrgPos *Pdrgpos() const
			{
				return m_pdrgpos;
			}

			// frame specifications
			DrgPwf *Pdrgpwf() const
			{
				return m_pdrgpwf;
			}

			// match function
			virtual
			BOOL FMatch(COperator *pop) const;

			// hashing function
			virtual
			ULONG UlHash() const;

			// sensitivity to order of inputs
			virtual
			BOOL FInputOrderSensitive() const
			{
				return true;
			}

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
				IMemoryPool *pmp,
				CExpressionHandle &exprhdl,
				CPartitionPropagationSpec *pppsRequired,
				ULONG ulChildIndex,
				DrgPdp *pdrgpdpCtxt,
				ULONG ulOptReq
				);

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

			// derive partition index map
			virtual
			CPartIndexMap *PpimDerive
				(
				IMemoryPool *, // pmp
				CExpressionHandle &exprhdl,
				CDrvdPropCtxt * //pdpctxt
				)
				const
			{
				return PpimPassThruOuter(exprhdl);
			}

			// derive partition filter map
			virtual
			CPartFilterMap *PpfmDerive
				(
				IMemoryPool *, // pmp
				CExpressionHandle &exprhdl
				)
				const
			{
				return PpfmPassThruOuter(exprhdl);
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
				) const;

			// return rewindability property enforcing type for this operator
			virtual
			CEnfdProp::EPropEnforcingType EpetRewindability
				(
				CExpressionHandle &, // exprhdl
				const CEnfdRewindability * // per
				) const;

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

			// print
			virtual
			IOstream &OsPrint(IOstream &os) const;

			// conversion function
			static
			CPhysicalSequenceProject *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopPhysicalSequenceProject == pop->Eopid());

				return dynamic_cast<CPhysicalSequenceProject*>(pop);
			}

	}; // class CPhysicalSequenceProject

}

#endif // !GPOPT_CPhysicalSequenceProject_H

// EOF
