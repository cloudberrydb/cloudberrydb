//	Greenplum Database
//	Copyright (C) 2016 Pivotal Software, Inc.

#ifndef GPOPT_CPhysicalUnionAll_H
#define GPOPT_CPhysicalUnionAll_H

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CDistributionSpecHashed.h"

#include "gpopt/operators/CPhysical.h"
#include "gpopt/operators/COperator.h"


namespace gpopt
{
	class CPhysicalUnionAll : public CPhysical
	{
		private:

			// output column array
			DrgPcr *const m_pdrgpcrOutput;

			// input column array
			DrgDrgPcr *const m_pdrgpdrgpcrInput;

			// if this union is needed for partial indexes then store the scan
			// id, otherwise this will be gpos::ulong_max
			const ULONG m_ulScanIdPartialIndex;

			// set representation of input columns
			DrgPcrs *m_pdrgpcrsInput;

			// array of child hashed distributions -- used locally for distribution derivation
			DrgPds *const m_pdrgpds;

			// map given array of scalar ident expressions to positions of UnionAll input columns in the given child;
			DrgPul *PdrgpulMap(IMemoryPool *pmp, DrgPexpr *pdrgpexpr, ULONG ulChildIndex) const;

			// derive hashed distribution from child operators
			CDistributionSpecHashed *PdshashedDerive(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

			// compute output hashed distribution matching the outer child's hashed distribution
			CDistributionSpecHashed *PdsMatching(IMemoryPool *pmp, const DrgPul *pdrgpulOuter) const;

			// derive output distribution based on child distribution
			CDistributionSpec *PdsDeriveFromChildren(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

		protected:

			CColRefSet *PcrsInput(ULONG ulChildIndex);

			// compute required hashed distribution of the n-th child
			CDistributionSpecHashed *
			PdshashedPassThru(IMemoryPool *pmp, CDistributionSpecHashed *pdshashedRequired, ULONG ulChildIndex) const;

		public:

			CPhysicalUnionAll
				(
					IMemoryPool *pmp,
					DrgPcr *pdrgpcrOutput,
					DrgDrgPcr *pdrgpdrgpcrInput,
					ULONG ulScanIdPartialIndex
				);

			virtual
			~CPhysicalUnionAll();

			// match function
			virtual
			BOOL FMatch(COperator *) const;

			// ident accessors
			virtual
			EOperatorId Eopid() const = 0;

			virtual
			const CHAR *SzId() const = 0;

			// sensitivity to order of inputs
			virtual
			BOOL FInputOrderSensitive() const;

			// accessor of output column array
			DrgPcr *PdrgpcrOutput() const;

			// accessor of input column array
			DrgDrgPcr *PdrgpdrgpcrInput() const;

			// if this unionall is needed for partial indexes then return the scan
			// id, otherwise return gpos::ulong_max
			ULONG UlScanIdPartialIndex() const;

			// is this unionall needed for a partial index
			BOOL FPartialIndex() const;

			// return true if operator passes through stats obtained from children,
			// this is used when computing stats during costing
			virtual
			BOOL FPassThruStats() const;

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

			// conversion function
			static
			CPhysicalUnionAll *PopConvert
				(
					COperator *pop
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

			// derive partition index map
			virtual
			CPartIndexMap *PpimDerive(IMemoryPool *pmp, CExpressionHandle &exprhdl, CDrvdPropCtxt *pdpctxt) const;

			// derive partition filter map
			virtual
			CPartFilterMap *PpfmDerive
				(
					IMemoryPool *pmp,
					CExpressionHandle &exprhdl
				)
			const;

			// derive rewindability
			virtual
			CRewindabilitySpec *PrsDerive(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

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
					CExpressionHandle &, // exprhdl
					const CEnfdRewindability * // per
				)
			const;

			// return partition propagation property enforcing type for this operator
			virtual
			CEnfdProp::EPropEnforcingType EpetPartitionPropagation
				(
					CExpressionHandle &exprhdl,
					const CEnfdPartitionPropagation *pepp
				)
			const;
	};
}

#endif
