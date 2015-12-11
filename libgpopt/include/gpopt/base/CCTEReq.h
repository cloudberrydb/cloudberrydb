//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CCTEReq.h
//
//	@doc:
//		CTE requirements. Each entry has a CTE id, whether it is a producer or
//		a consumer, whether it is required or optional (a required CTE has to
//		be in the derived CTE map of that subtree, while an optional CTE may or
//		may not be there). If the CTE entry represents a consumer, then the
//		plan properties of the corresponding producer are also part of that entry
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CCTEReq_H
#define GPOPT_CCTEReq_H

#include "gpos/base.h"
#include "gpos/common/CHashMap.h"
#include "gpos/common/CHashMapIter.h"
#include "gpos/common/CRefCount.h"

#include "gpopt/base/CCTEMap.h"
#include "gpopt/base/CDrvdPropPlan.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CCTEReq
	//
	//	@doc:
	//		CTE requirements
	//
	//---------------------------------------------------------------------------
	class CCTEReq : public CRefCount
	{

		private:

			//---------------------------------------------------------------------------
			//	@class:
			//		CCTEReqEntry
			//
			//	@doc:
			//		A single entry in the CTE requirement
			//
			//---------------------------------------------------------------------------
			class CCTEReqEntry : public CRefCount
			{

				private:

					// cte id
					ULONG m_ulId;

					// cte type
					CCTEMap::ECteType m_ect;

					// is it required or optional
					BOOL m_fRequired;

					// plan properties of corresponding producer
					CDrvdPropPlan *m_pdpplan;

					// private copy ctor
					CCTEReqEntry(const CCTEReqEntry&);

				public:

					// ctor
					CCTEReqEntry(ULONG ulId, CCTEMap::ECteType ect, BOOL fRequired, CDrvdPropPlan *pdpplan);

					// dtor
					virtual
					~CCTEReqEntry();

					// cte id
					ULONG UlId() const
					{
						return m_ulId;
					}

					// cte type
					CCTEMap::ECteType Ect() const
					{
						return m_ect;
					}

					// required flag
					BOOL FRequired() const
					{
						return m_fRequired;
					}

					// plan properties
					CDrvdPropPlan *PdpplanProducer() const
					{
						return m_pdpplan;
					}

					// hash function
					ULONG UlHash() const;

					// equality function
					BOOL FEqual(CCTEReqEntry *pcre) const;

					// print function
					virtual
					IOstream &OsPrint(IOstream &os) const;

			}; // class CCTEReqEntry

			// map CTE id to CTE Req entry
			typedef CHashMap<ULONG, CCTEReqEntry, gpos::UlHash<ULONG>, gpos::FEqual<ULONG>,
				CleanupDelete<ULONG>, CleanupRelease<CCTEReqEntry> > HMCteReq;

			// map iterator
			typedef CHashMapIter<ULONG, CCTEReqEntry, gpos::UlHash<ULONG>, gpos::FEqual<ULONG>,
				CleanupDelete<ULONG>, CleanupRelease<CCTEReqEntry> > HMCteReqIter;

			// memory pool
			IMemoryPool *m_pmp;

			// cte map
			HMCteReq *m_phmcter;

			// required cte ids (not optional)
			DrgPul* m_pdrgpulRequired;

			// private copy ctor
			CCTEReq(const CCTEReq&);

			// lookup info for given cte id
			CCTEReqEntry *PcreLookup(ULONG ulCteId) const;

		public:

			// ctor
			explicit
			CCTEReq(IMemoryPool *pmp);

			// dtor
			virtual
			~CCTEReq();

			// required cte ids
			DrgPul *PdrgpulRequired() const
			{
				return m_pdrgpulRequired;
			}

			// return the CTE type associated with the given ID in the requirements
			CCTEMap::ECteType Ect(const ULONG ulId) const;

			// insert a new entry, no entry with the same id can already exist
			void Insert(ULONG ulCteId, CCTEMap::ECteType ect, BOOL fRequired, CDrvdPropPlan *pdpplan);

			// insert a new consumer entry with the given id. The plan properties are
			// taken from the given context
			void InsertConsumer(ULONG ulId, DrgPdp *pdrgpdpCtxt);

			// check if two cte requirements are equal
			BOOL FEqual
					(
					const CCTEReq *pcter
					)
					const
			{
				GPOS_ASSERT(NULL != pcter);
				return this->FSubset(pcter) && pcter->FSubset(this);
			}

			// check if current requirement is a subset of the given one
			BOOL FSubset(const CCTEReq *pcter) const;

			// check if the given CTE is in the requirements
			BOOL FContainsRequirement(const ULONG ulId, const CCTEMap::ECteType ect) const;

			// hash function
			ULONG UlHash() const;

			// returns a new requirement containing unresolved CTE requirements given a derived CTE map
			CCTEReq *PcterUnresolved(IMemoryPool *pmp, CCTEMap *pcm);

			// unresolved CTE requirements given a derived CTE map for a sequence
			// operator
			CCTEReq *PcterUnresolvedSequence(IMemoryPool *pmp, CCTEMap *pcm, DrgPdp *pdrgpdpCtxt);

			// create a copy of the current requirement where all the entries are marked optional
			CCTEReq *PcterAllOptional(IMemoryPool *pmp);

			// lookup plan properties for given cte id
			CDrvdPropPlan *Pdpplan (ULONG ulCteId) const;

			// print function
			virtual
			IOstream &OsPrint(IOstream &os) const;

	}; // class CCTEMap

 	// shorthand for printing
	IOstream &operator << (IOstream &os, CCTEReq &cter);

}

#endif // !GPOPT_CCTEMap_H

// EOF
