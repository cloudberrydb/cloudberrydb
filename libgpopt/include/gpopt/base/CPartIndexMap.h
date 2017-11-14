//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPartIndexMap.h
//
//	@doc:
//		Mapping of partition index to a manipulator type
//---------------------------------------------------------------------------
#ifndef GPOPT_CPartIndexMap_H
#define GPOPT_CPartIndexMap_H

#include "gpos/base.h"
#include "gpos/common/CHashMap.h"
#include "gpos/common/CHashMapIter.h"
#include "gpos/common/CRefCount.h"

#include "gpopt/base/CColRef.h"
#include "gpopt/base/CPartKeys.h"
#include "gpopt/metadata/CPartConstraint.h"

// fwd decl
namespace gpmd
{
	class IMDId;
}

namespace gpopt
{
	using namespace gpos;
	using namespace gpmd;

	// fwd decl
	class CPartitionPropagationSpec;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CPartIndexMap
	//
	//	@doc:
	//		Mapping of partition index to a manipulator type
	//
	//---------------------------------------------------------------------------
	class CPartIndexMap : public CRefCount
	{
		public:
			// types of partition index id manipulators
			enum EPartIndexManipulator
			{
				EpimPropagator,
				EpimConsumer,
				EpimResolver,

				EpimSentinel
			};

			enum EPartPropagationRequestAction
			{
				EppraPreservePropagators,
				EppraIncrementPropagators,
				EppraZeroPropagators,
				EppraSentinel
			};

		private:

			//---------------------------------------------------------------------------
			//	@class:
			//		CPartTableInfo
			//
			//	@doc:
			//		Partition index map entry
			//
			//---------------------------------------------------------------------------
			class CPartTableInfo : public CRefCount
			{
				private:

					// scan id
					ULONG m_ulScanId;

					// part constraints for partial scans and partition resolvers indexed
					// by the scan id
					PartCnstrMap *m_ppartcnstrmap;

					// manipulator type
					EPartIndexManipulator m_epim;

					// does this part table item contain partial scans
					BOOL m_fPartialScans;

					// partition table mdid
					IMDId *m_pmdid;

					// partition keys
					DrgPpartkeys *m_pdrgppartkeys;

					// part constraint of the relation
					CPartConstraint *m_ppartcnstrRel;

					// number of propagators to expect - this is only valid if the
					// manipulator type is Consumer
					ULONG m_ulPropagators;

					// description of manipulator types
					static
					const CHAR *m_szManipulator[EpimSentinel];

					// add a part constraint
					void AddPartConstraint(IMemoryPool *pmp, ULONG ulPartIndexId, CPartConstraint *ppartcnstr);

					// private copy ctor
					CPartTableInfo(const CPartTableInfo &);

					// does the given part constraint map define partial scans
					static
					BOOL FDefinesPartialScans(PartCnstrMap *ppartcnstrmap, CPartConstraint *ppartcnstrRel);

				public:

					// ctor
					CPartTableInfo
						(
						ULONG ulScanId,
						PartCnstrMap *ppartcnstrmap,
						EPartIndexManipulator epim,
						IMDId *pmdid,
						DrgPpartkeys *pdrgppartkeys,
						CPartConstraint *ppartcnstrRel,
						ULONG ulPropagators
						);

					//dtor
					virtual
					~CPartTableInfo();

					// partition index accessor
					virtual
					ULONG UlScanId() const
					{
						return m_ulScanId;
					}

					// part constraint map accessor
					PartCnstrMap *Ppartcnstrmap() const
					{
						return m_ppartcnstrmap;
					}

					// relation part constraint
					CPartConstraint *PpartcnstrRel() const
					{
						return m_ppartcnstrRel;
					}

					// expected number of propagators
					ULONG UlExpectedPropagators() const
					{
						return m_ulPropagators;
					}

					// set the number of expected propagators
					void SetExpectedPropagators(ULONG ulPropagators)
					{
						m_ulPropagators = ulPropagators;
					}

					// manipulator type accessor
					virtual
					EPartIndexManipulator Epim() const
					{
						return m_epim;
					}

					// partial scans accessor
					virtual
					BOOL FPartialScans() const
					{
						return m_fPartialScans;
					}

					// mdid of partition table
					virtual
					IMDId *Pmdid() const
					{
						return m_pmdid;
					}

					// partition keys of partition table
					virtual
					DrgPpartkeys *Pdrgppartkeys() const
					{
						return m_pdrgppartkeys;
					}

					static
					const CHAR *SzManipulatorType(EPartIndexManipulator epim);

					// add part constraints
					void AddPartConstraints(IMemoryPool *pmp, PartCnstrMap *ppartcnstrmap);

					IOstream &OsPrint(IOstream &os) const;

#ifdef GPOS_DEBUG
					// debug print for interactive debugging sessions only
					void DbgPrint() const;
#endif // GPOS_DEBUG

			}; // CPartTableInfo

			// map scan id to partition table info entry
			typedef CHashMap<ULONG, CPartTableInfo, gpos::UlHash<ULONG>, gpos::FEqual<ULONG>,
				CleanupDelete<ULONG>, CleanupRelease<CPartTableInfo> > PartIndexMap;

			// map iterator
			typedef CHashMapIter<ULONG, CPartTableInfo, gpos::UlHash<ULONG>, gpos::FEqual<ULONG>,
				CleanupDelete<ULONG>, CleanupRelease<CPartTableInfo> > PartIndexMapIter;

			// memory pool
			IMemoryPool *m_pmp;

			// partition index map
			PartIndexMap *m_pim;

			// number of unresolved entries
			ULONG m_ulUnresolved;
			
			// number of unresolved entries with zero expected propagators
			ULONG m_ulUnresolvedZeroPropagators;

			// private copy ctor
			CPartIndexMap(const CPartIndexMap&);

			// lookup info for given scan id
			CPartTableInfo *PptiLookup(ULONG ulScanId) const;

			// check if part index map entry satisfies the corresponding required
			// partition propagation spec entry
			BOOL FSatisfiesEntry(const CPartTableInfo *pptiReqd, CPartTableInfo *pptiDrvd) const;

			// handle the cases where one of the given manipulators is a propagator and the other is a consumer
			static
			void ResolvePropagator
				(
				EPartIndexManipulator epimFst,
				ULONG ulExpectedPropagatorsFst,
				EPartIndexManipulator epimSnd,
				ULONG ulExpectedPropagatorsSnd,
				EPartIndexManipulator *pepimResult,
				ULONG *pulExpectedPropagatorsResult
				);

			// helper to add part-index id's found in first map and are unresolved based on second map
			static
			void AddUnresolved(IMemoryPool *pmp, const CPartIndexMap &pimFst, const CPartIndexMap &pimSnd, CPartIndexMap* ppimResult);

			// print part constraint map
			static
			IOstream &OsPrintPartCnstrMap(ULONG ulPartIndexId, PartCnstrMap *ppartcnstrmap, IOstream &os);
			
		public:

			// ctor
			explicit
			CPartIndexMap(IMemoryPool *pmp);

			// dtor
			virtual
			~CPartIndexMap();

			// inserting a new map entry
			void Insert
				(
				ULONG ulScanId,
				PartCnstrMap *ppartcnstrmap, 
				EPartIndexManipulator epim,
				ULONG ulExpectedPropagators,
				IMDId *pmdid, 
				DrgPpartkeys *pdrgppartkeys,
				CPartConstraint *ppartcnstrRel
				);

			// does map contain unresolved entries?
			BOOL FContainsUnresolved() const;

			// does map contain unresolved entries with zero propagators?
			BOOL FContainsUnresolvedZeroPropagators() const;

			// extract scan ids in the given memory pool
			DrgPul *PdrgpulScanIds(IMemoryPool *pmp, BOOL fConsumersOnly = false) const;

			// check if two part index maps are equal
			BOOL FEqual
				(
				const CPartIndexMap *ppim
				) 
				const
			{
				return  (m_pim->UlEntries() == ppim->m_pim->UlEntries()) &&
						ppim->FSubset(this);
			}
			
			// hash function
			ULONG UlHash() const;
			
			// check if partition index map satsfies required partition propagation spec
			BOOL FSatisfies(const CPartitionPropagationSpec *ppps) const;
			
			// check if current part index map is a subset of the given one
			BOOL FSubset(const CPartIndexMap *ppim) const;

			// check if part index map contains the given scan id
			BOOL FContains
				(
				ULONG ulScanId
				)
				const
			{
				return NULL != m_pim->PtLookup(&ulScanId);
			}

			// check if the given expression derives unneccessary partition selectors
			BOOL FContainsRedundantPartitionSelectors(CPartIndexMap *ppimReqd) const;

			// part keys of the entry with the given scan id
			DrgPpartkeys *Pdrgppartkeys(ULONG ulScanId) const;

			// relation mdid of the entry with the given scan id
			IMDId *PmdidRel(ULONG ulScanId) const;

			// part constraint map of the entry with the given scan id
			PartCnstrMap *Ppartcnstrmap(ULONG ulScanId) const;

			// relation part constraint of the entry with the given scan id
			CPartConstraint *PpartcnstrRel(ULONG ulScanId) const;

			// manipulator type of the entry with the given scan id
			EPartIndexManipulator Epim(ULONG ulScanId) const;

			// number of expected propagators of the entry with the given scan id
			ULONG UlExpectedPropagators(ULONG ulScanId) const;

			// set the number of expected propagators for the entry with the given scan id
			void SetExpectedPropagators(ULONG ulScanId, ULONG ulPropagators);

			// check whether the entry with the given scan id has partial scans
			BOOL FPartialScans(ULONG ulScanId) const;

			// get part consumer with given scanId from the given map, and add it to the
			// current map with the given array of keys
			void AddRequiredPartPropagation(CPartIndexMap *ppimSource, ULONG ulScanId, EPartPropagationRequestAction eppra, DrgPpartkeys *pdrgppartkeys = NULL);

			// return a new part index map for a partition selector with the given
			// scan id, and the given number of expected selectors above it
			CPartIndexMap *PpimPartitionSelector(IMemoryPool *pmp, ULONG ulScanId, ULONG ulExpectedFromReq) const;

			// print function
			virtual
			IOstream &OsPrint(IOstream &os) const;

			// combine the two given maps and return the resulting map
			static
			CPartIndexMap *PpimCombine(IMemoryPool *pmp, const CPartIndexMap &pimFst, const CPartIndexMap &pimSnd);

#ifdef GPOS_DEBUG
			// debug print for interactive debugging sessions only
			void DbgPrint() const;
#endif // GPOS_DEBUG

	}; // class CPartIndexMap

 	// shorthand for printing
	IOstream &operator << (IOstream &os, CPartIndexMap &pim);

}

#endif // !GPOPT_CPartIndexMap_H

// EOF
