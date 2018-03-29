//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CCTEInfo.h
//
//	@doc:
//		Information about CTEs in a query
//---------------------------------------------------------------------------
#ifndef GPOPT_CCTEInfo_H
#define GPOPT_CCTEInfo_H

#include "gpos/base.h"
#include "gpos/common/CHashMap.h"
#include "gpos/common/CStack.h"
#include "gpos/sync/CMutex.h"
#include "gpos/sync/CAtomicCounter.h"

#include "gpopt/base/CColumnFactory.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/base/CColRefSet.h"

namespace gpopt
{

	// fwd declarations
	class CLogicalCTEConsumer;

	// hash map: CColRef -> ULONG
	typedef CHashMap<CColRef, ULONG, gpos::UlHash<CColRef>, gpos::FEqual<CColRef>,
					CleanupNULL<CColRef>, CleanupDelete<ULONG> > HMCrUl;

	//---------------------------------------------------------------------------
	//	@class:
	//		CCTEInfo
	//
	//	@doc:
	//		Global information about common table expressions (CTEs) including:
	//		- the expression tree that defines each CTE
	//		- the number of consumers created by the optimizer
	//		- a mapping from consumer columns to producer columns
	//
	//---------------------------------------------------------------------------
	class CCTEInfo : public CRefCount
	{
		private:

			//-------------------------------------------------------------------
			//	@struct:
			//		SConsumerCounter
			//
			//	@doc:
			//		Representation of the number of consumers of a given CTE inside
			// 		a specific context (e.g. inside the main query, inside another CTE, etc.)
			//
			//-------------------------------------------------------------------
			struct SConsumerCounter
			{
				private:

					// consumer ID
					ULONG m_ulCTEId;

					// number of occurrences
					ULONG m_ulCount;

				public:

					// mutex for locking entry when changing member variables
					CMutex m_mutex;

					// ctor
					explicit
					SConsumerCounter
						(
						ULONG ulCTEId
						)
						:
						m_ulCTEId(ulCTEId),
						m_ulCount(1)
					{}

					// consumer ID
					ULONG UlCTEId() const
					{
						return m_ulCTEId;
					}

					// number of consumers
					ULONG UlCount() const
					{
						return m_ulCount;
					}

					// increment number of consumers
					void Increment()
					{
						m_ulCount++;
					}
			};

			// hash map mapping ULONG -> SConsumerCounter
			typedef CHashMap<ULONG, SConsumerCounter, gpos::UlHash<ULONG>, gpos::FEqual<ULONG>,
					CleanupDelete<ULONG>, CleanupDelete<SConsumerCounter> > HMUlConsumerMap;

			// map iterator
			typedef CHashMapIter<ULONG, SConsumerCounter, gpos::UlHash<ULONG>, gpos::FEqual<ULONG>,
							CleanupDelete<ULONG>, CleanupDelete<SConsumerCounter> > HMUlConsumerMapIter;

			// hash map mapping ULONG -> HMUlConsumerMap: maps from CTE producer ID to all consumers inside this CTE
			typedef CHashMap<ULONG, HMUlConsumerMap, gpos::UlHash<ULONG>, gpos::FEqual<ULONG>,
					CleanupDelete<ULONG>, CleanupRelease<HMUlConsumerMap> > HMUlProdConsMap;

			//-------------------------------------------------------------------
			//	@struct:
			//		CCTEInfoEntry
			//
			//	@doc:
			//		A single entry for CTEInfo, representing a single CTE producer
			//
			//-------------------------------------------------------------------
			class CCTEInfoEntry : public CRefCount
			{
				private:

					// memory pool
					IMemoryPool *m_pmp;

					// logical producer expression
					CExpression *m_pexprCTEProducer;

					// map columns of all created consumers of current CTE to their positions in consumer output
					HMCrUl *m_phmcrulConsumers;

					// is this CTE used
					BOOL m_fUsed;

				public:

					// mutex for locking entry when changing member variables when deriving stats
					CMutex m_mutex;

					// ctors
					CCTEInfoEntry(IMemoryPool *pmp, CExpression *pexprCTEProducer);
					CCTEInfoEntry(IMemoryPool *pmp, CExpression *pexprCTEProducer, BOOL fUsed);

					// dtor
					~CCTEInfoEntry();

					// CTE expression
					CExpression *Pexpr() const
					{
						return m_pexprCTEProducer;
					}

					// is this CTE used?
					BOOL FUsed() const
					{
						return m_fUsed;
					}

					// CTE id
					ULONG UlCTEId() const;

					// mark CTE as unused
					void MarkUnused()
					{
						m_fUsed = false;
					}

					// add given columns to consumers column map
					void AddConsumerCols(DrgPcr *pdrgpcr);

					// return position of given consumer column in consumer output
					ULONG UlConsumerColPos(CColRef *pcr);

			}; //class CCTEInfoEntry

			// hash maps mapping ULONG -> CCTEInfoEntry
			typedef CHashMap<ULONG, CCTEInfoEntry, gpos::UlHash<ULONG>, gpos::FEqual<ULONG>,
					CleanupDelete<ULONG>, CleanupRelease<CCTEInfoEntry> > HMUlCTEInfoEntry;

			// map iterator
			typedef CHashMapIter<ULONG, CCTEInfoEntry, gpos::UlHash<ULONG>, gpos::FEqual<ULONG>,
							CleanupDelete<ULONG>, CleanupRelease<CCTEInfoEntry> > HMUlCTEInfoEntryIter;

			// memory pool
			IMemoryPool *m_pmp;

			// mapping from cte producer id -> cte info entry
			HMUlCTEInfoEntry *m_phmulcteinfoentry;

			// next available CTE Id
			CAtomicULONG m_ulNextCTEId;

			// whether or not to inline CTE consumers
			BOOL m_fEnableInlining;

			// consumers inside each cte/main query
			HMUlProdConsMap *m_phmulprodconsmap;

			// initialize default statistics for a given CTE Producer
			void InitDefaultStats(CExpression *pexprCTEProducer);

			// preprocess CTE producer expression
			CExpression *PexprPreprocessCTEProducer(const CExpression *pexprCTEProducer);

			// private copy ctor
			CCTEInfo(const CCTEInfo &);

			// number of consumers of given CTE inside a given parent
			ULONG UlConsumersInParent(ULONG ulConsumerId, ULONG ulParentId) const;

			// find all CTE consumers inside given parent, and push them to the given stack
			void FindConsumersInParent(ULONG ulParentId, CBitSet *pbsUnusedConsumers, CStack<ULONG> *pstack);

		public:
			// ctor
			explicit
			CCTEInfo(IMemoryPool *pmp);

			//dtor
			virtual
			~CCTEInfo();

			// logical cte producer with given id
			CExpression *PexprCTEProducer(ULONG ulCTEId) const;

			// number of CTE consumers of given CTE
			ULONG UlConsumers(ULONG ulCTEId) const;

			// check if given CTE is used
			BOOL FUsed(ULONG ulCTEId) const;

			// increment number of CTE consumers
			void
			IncrementConsumers(ULONG ulConsumerId,
							   ULONG ulParentCTEId = gpos::ulong_max);

			// add cte producer to hashmap
			void AddCTEProducer(CExpression *pexprCTEProducer);

			// replace cte producer with given expression
			void ReplaceCTEProducer(CExpression *pexprCTEProducer);

			// next available CTE id
			ULONG UlNextId()
			{
				return m_ulNextCTEId.TIncr();
			}

			// derive the statistics on the CTE producer
			void DeriveProducerStats
				(
				CLogicalCTEConsumer *popConsumer, // CTE Consumer operator
				CColRefSet *pcrsStat // required stat columns on the CTE Consumer
				);

			// return a CTE requirement with all the producers as optional
			CCTEReq *PcterProducers(IMemoryPool *pmp) const;

			// return an array of all stored CTE expressions
			DrgPexpr *PdrgPexpr(IMemoryPool *pmp) const;

			// disable CTE inlining
			void DisableInlining()
			{
				m_fEnableInlining = false;
			}

			// whether or not to inline CTE consumers
			BOOL FEnableInlining() const
			{
				return m_fEnableInlining;
			}

			// mark unused CTEs
			void MarkUnusedCTEs();

			// walk the producer expressions and add the mapping between computed column
			// and their corresponding used column(s)
			void MapComputedToUsedCols(CColumnFactory *pcf) const;

			// add given columns to consumers column map
			void AddConsumerCols(ULONG ulCTEId, DrgPcr *pdrgpcr);

			// return position of given consumer column in consumer output
			ULONG UlConsumerColPos(ULONG ulCTEId, CColRef *pcr);

			// return a map from Id's of consumer columns in the given column set to their corresponding producer columns
			HMUlCr *PhmulcrConsumerToProducer(IMemoryPool *pmp, ULONG ulCTEId, CColRefSet *pcrs, DrgPcr *pdrgpcrProducer);

	}; // CCTEInfo
}

#endif // !GPOPT_CCTEInfo_H

// EOF

