//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CPartInfo.h
//
//	@doc:
//		Derived partition information at the logical level
//---------------------------------------------------------------------------
#ifndef GPOPT_CPartInfo_H
#define GPOPT_CPartInfo_H

#include "gpos/base.h"
#include "gpopt/base/CColRef.h"
#include "gpopt/base/CPartKeys.h"

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
	class CPartConstraint;

	//---------------------------------------------------------------------------
	//	@class:
	//		CPartInfo
	//
	//	@doc:
	//		Derived partition information at the logical level
	//
	//---------------------------------------------------------------------------
	class CPartInfo : public CRefCount
	{
		private:

			//---------------------------------------------------------------------------
			//	@class:
			//		CPartInfoEntry
			//
			//	@doc:
			//		A single entry of the CPartInfo
			//
			//---------------------------------------------------------------------------
			class CPartInfoEntry : public CRefCount
			{

				private:

					// scan id
					ULONG m_ulScanId;

					// partition table mdid
					IMDId *m_pmdid;

					// partition keys
					DrgPpartkeys *m_pdrgppartkeys;

					// part constraint of the relation
					CPartConstraint *m_ppartcnstrRel;

					// private copy ctor
					CPartInfoEntry(const CPartInfoEntry &);

				public:

					// ctor
					CPartInfoEntry(ULONG ulScanId, IMDId *pmdid, DrgPpartkeys *pdrgppartkeys, CPartConstraint *ppartcnstrRel);

					// dtor
					virtual
					~CPartInfoEntry();

					// scan id
					virtual
					ULONG UlScanId() const
					{
						return m_ulScanId;
					}

					// relation part constraint
					CPartConstraint *PpartcnstrRel() const
					{
						return m_ppartcnstrRel;
					}

					// create a copy of the current object, and add a set of remapped
					// part keys to this entry, using the existing keys and the given hashmap
					CPartInfoEntry *PpartinfoentryAddRemappedKeys(IMemoryPool *pmp, CColRefSet *pcrs, HMUlCr *phmulcr);

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

					// print function
					IOstream &OsPrint(IOstream &os) const;

					// copy part info entry into given memory pool
					CPartInfoEntry *PpartinfoentryCopy(IMemoryPool *pmp);

#ifdef GPOS_DEBUG
					// debug print for interactive debugging sessions only
					void DbgPrint() const;
#endif // GPOS_DEBUG

			}; // CPartInfoEntry

			typedef CDynamicPtrArray<CPartInfoEntry, CleanupRelease> DrgPpartentries;

			// partition table consumers
			DrgPpartentries *m_pdrgppartentries;

			// private ctor
			explicit
			CPartInfo(DrgPpartentries *pdrgppartentries);

			//private copy ctor
			CPartInfo(const CPartInfo &);

		public:

			// ctor
			explicit
			CPartInfo(IMemoryPool *pmp);

			// dtor
			virtual
			~CPartInfo();

			// number of part table consumers
			ULONG UlConsumers() const
			{
				return m_pdrgppartentries->UlLength();
			}

			// add part table consumer
			void AddPartConsumer
				(
				IMemoryPool *pmp,
				ULONG ulScanId,
				IMDId *pmdid,
				DrgDrgPcr *pdrgpdrgpcrPart,
				CPartConstraint *ppartcnstrRel
				);

			// scan id of the entry at the given position
			ULONG UlScanId(ULONG ulPos)	const;

			// relation mdid of the entry at the given position
			IMDId *PmdidRel(ULONG ulPos) const;

			// part keys of the entry at the given position
			DrgPpartkeys *Pdrgppartkeys(ULONG ulPos) const;

			// part constraint of the entry at the given position
			CPartConstraint *Ppartcnstr(ULONG ulPos) const;

			// check if part info contains given scan id
			BOOL FContainsScanId(ULONG ulScanId) const;

			// part keys of the entry with the given scan id
			DrgPpartkeys *PdrgppartkeysByScanId(ULONG ulScanId) const;

			// return a new part info object with an additional set of remapped keys
			CPartInfo *PpartinfoWithRemappedKeys
				(
				IMemoryPool *pmp,
				DrgPcr *pdrgpcrSrc,
				DrgPcr *pdrgpcrDest
				)
				const;

			// print
			IOstream &OsPrint(IOstream &) const;

			// combine two part info objects
			static
			CPartInfo *PpartinfoCombine
				(
				IMemoryPool *pmp,
				CPartInfo *ppartinfoFst,
				CPartInfo *ppartinfoSnd
				);

#ifdef GPOS_DEBUG
			// debug print for interactive debugging sessions only
			void DbgPrint() const;
#endif // GPOS_DEBUG

	}; // CPartInfo

	// shorthand for printing
	inline
	IOstream &operator << (IOstream &os, CPartInfo &partinfo)
	{
		return partinfo.OsPrint(os);
	}
}

#endif // !GPOPT_CPartInfo_H

// EOF

