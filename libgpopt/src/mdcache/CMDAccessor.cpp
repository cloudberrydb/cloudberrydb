//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CMDAccessor.cpp
//
//	@doc:
//		Implementation of the metadata accessor class handling accesses to
//		metadata objects in an optimization session
//---------------------------------------------------------------------------

#include "gpos/common/CAutoP.h"
#include "gpos/common/CAutoRef.h"
#include "gpos/common/CTimerUser.h"
#include "gpos/io/COstreamString.h"
#include "gpos/task/CAutoSuspendAbort.h"
#include "gpos/error/CAutoTrace.h"

#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CColRefTable.h"
#include "gpopt/exception.h"
#include "gpopt/mdcache/CMDAccessor.h"
#include "gpopt/mdcache/CMDAccessorUtils.h"


#include "naucrates/exception.h"
#include "naucrates/traceflags/traceflags.h"

#include "naucrates/dxl/CDXLUtils.h"

#include "naucrates/md/IMDCacheObject.h"
#include "naucrates/md/IMDRelation.h"
#include "naucrates/md/IMDRelationExternal.h"
#include "naucrates/md/IMDType.h"
#include "naucrates/md/IMDScalarOp.h"
#include "naucrates/md/IMDFunction.h"
#include "naucrates/md/IMDAggregate.h"
#include "naucrates/md/IMDIndex.h"
#include "naucrates/md/IMDTrigger.h"
#include "naucrates/md/IMDCheckConstraint.h"
#include "naucrates/md/IMDRelStats.h"
#include "naucrates/md/IMDColStats.h"
#include "naucrates/md/IMDCast.h"
#include "naucrates/md/IMDScCmp.h"

#include "naucrates/md/CMDIdRelStats.h"
#include "naucrates/md/CMDIdColStats.h"
#include "naucrates/md/CMDIdCast.h"
#include "naucrates/md/CMDIdScCmp.h"

#include "naucrates/md/IMDProvider.h"
#include "naucrates/md/CMDProviderGeneric.h"

using namespace gpos;
using namespace gpmd;
using namespace gpopt;
using namespace gpdxl;

// no. of hashtable buckets
#define GPOPT_CACHEACC_HT_NUM_OF_BUCKETS 128

// static member initialization

// invalid mdid pointer
const MdidPtr CMDAccessor::SMDAccessorElem::m_pmdidInvalid = NULL;

// invalid md provider element
const CMDAccessor::SMDProviderElem CMDAccessor::SMDProviderElem::m_mdpelemInvalid (CSystemId(IMDId::EmdidSentinel, NULL, 0), NULL);

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::SMDAccessorElem::SMDAccessorElem
//
//	@doc:
//		Constructs a metadata accessor element for the accessors hashtable
//
//---------------------------------------------------------------------------
CMDAccessor::SMDAccessorElem::SMDAccessorElem
	(
	IMDCacheObject *pimdobj,
	IMDId *pmdid
	)
	:
	m_pimdobj(pimdobj),
	m_pmdid(pmdid)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::SMDAccessorElem::~SMDAccessorElem
//
//	@doc:
//		Destructor for the metadata accessor element
//
//---------------------------------------------------------------------------
CMDAccessor::SMDAccessorElem::~SMDAccessorElem()
{
	// deleting the cache accessor will effectively unpin the cache entry for that object
	m_pimdobj->Release();
	m_pmdid->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::SMDAccessorElem::Pmdid
//
//	@doc:
//		Return the key for this hashtable element
//
//---------------------------------------------------------------------------
IMDId *
CMDAccessor::SMDAccessorElem::Pmdid()
{
	return m_pmdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::SMDAccessorElem::FEqual
//
//	@doc:
//		Equality function for cache accessors hash table
//
//---------------------------------------------------------------------------
BOOL 
CMDAccessor::SMDAccessorElem::FEqual
	(
	const MdidPtr &pmdidLeft,
	const MdidPtr &pmdidRight
	)
{
	if (pmdidLeft == m_pmdidInvalid || pmdidRight == m_pmdidInvalid)
	{
		return pmdidLeft == m_pmdidInvalid && pmdidRight == m_pmdidInvalid;
	}

	return pmdidLeft->FEquals(pmdidRight);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::SMDAccessorElem::UlHash
//
//	@doc:
//		Hash function for cache accessors hash table
//
//---------------------------------------------------------------------------
ULONG 
CMDAccessor::SMDAccessorElem::UlHash
	(
	const MdidPtr& pmdid
	)
{
	GPOS_ASSERT(m_pmdidInvalid != pmdid);

	return pmdid->UlHash();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::SMDProviderElem::SMDProviderElem
//
//	@doc:
//		Constructs an MD provider element
//
//---------------------------------------------------------------------------
CMDAccessor::SMDProviderElem::SMDProviderElem
	(
	CSystemId sysid,
	IMDProvider *pmdp
	)
	:
	m_sysid(sysid),
	m_pmdp(pmdp)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::SMDProviderElem::~SMDProviderElem
//
//	@doc:
//		Destructor for the MD provider element
//
//---------------------------------------------------------------------------
CMDAccessor::SMDProviderElem::~SMDProviderElem()
{
	CRefCount::SafeRelease(m_pmdp);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::SMDProviderElem::Pmdp
//
//	@doc:
//		Returns the MD provider for this hash table element
//
//---------------------------------------------------------------------------
IMDProvider *
CMDAccessor::SMDProviderElem::Pmdp()
{
	return m_pmdp;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::SMDProviderElem::Sysid
//
//	@doc:
//		Returns the system id for this hash table element
//
//---------------------------------------------------------------------------
CSystemId
CMDAccessor::SMDProviderElem::Sysid() const
{
	return m_sysid;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::SMDProviderElem::FEqual
//
//	@doc:
//		Equality function for hash tables
//
//---------------------------------------------------------------------------
BOOL 
CMDAccessor::SMDProviderElem::FEqual
	(
	const SMDProviderElem &mdpelemLeft,
	const SMDProviderElem &mdpelemRight
	)
{
	return mdpelemLeft.m_sysid.FEquals(mdpelemRight.m_sysid);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::SMDProviderElem::UlHash
//
//	@doc:
//		Hash function for cost contexts hash table
//
//---------------------------------------------------------------------------
ULONG 
CMDAccessor::SMDProviderElem::UlHash
	(
	const SMDProviderElem &mdpelem
	)
{
	GPOS_ASSERT(!FEqual(mdpelem, m_mdpelemInvalid));

	return mdpelem.m_sysid.UlHash();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::CMDAccessor
//
//	@doc:
//		Constructs a metadata accessor
//
//---------------------------------------------------------------------------
CMDAccessor::CMDAccessor
	(
	IMemoryPool *pmp,
	MDCache *pcache
	)
	:
	m_pmp(pmp),
	m_pcache(pcache),
	m_dLookupTime(0.0),
	m_dFetchTime(0.0)
{
	GPOS_ASSERT(NULL != m_pmp);
	GPOS_ASSERT(NULL != m_pcache);
	
	m_pmdpGeneric = GPOS_NEW(pmp) CMDProviderGeneric(pmp);
	
	InitHashtables(pmp);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::CMDAccessor
//
//	@doc:
//		Constructs a metadata accessor and registers an MD provider
//
//---------------------------------------------------------------------------
CMDAccessor::CMDAccessor
	(
	IMemoryPool *pmp,
	MDCache *pcache,
	CSystemId sysid,
	IMDProvider *pmdp
	)
	:
	m_pmp(pmp),
	m_pcache(pcache),
	m_dLookupTime(0.0),
	m_dFetchTime(0.0)
{
	GPOS_ASSERT(NULL != m_pmp);
	GPOS_ASSERT(NULL != m_pcache);
	
	m_pmdpGeneric = GPOS_NEW(pmp) CMDProviderGeneric(pmp);
	
	InitHashtables(pmp);
	
	RegisterProvider(sysid, pmdp);
}


//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::CMDAccessor
//
//	@doc:
//		Constructs a metadata accessor and registers MD providers
//
//---------------------------------------------------------------------------
CMDAccessor::CMDAccessor
	(
	IMemoryPool *pmp,
	MDCache *pcache,
	const DrgPsysid *pdrgpsysid,
	const DrgPmdp *pdrgpmdp
	)
	:
	m_pmp(pmp),
	m_pcache(pcache),
	m_dLookupTime(0.0),
	m_dFetchTime(0.0)
{
	GPOS_ASSERT(NULL != m_pmp);
	GPOS_ASSERT(NULL != m_pcache);

	m_pmdpGeneric = GPOS_NEW(pmp) CMDProviderGeneric(pmp);

	InitHashtables(pmp);

	RegisterProviders(pdrgpsysid, pdrgpmdp);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::DestroyAccessorElement
//
//	@doc:
//		Destroy accessor element;
//		called only at destruction time
//
//---------------------------------------------------------------------------
void
CMDAccessor::DestroyAccessorElement
	(
	SMDAccessorElem *pmdaccelem
	)
{
	GPOS_ASSERT(NULL != pmdaccelem);

	// remove deletion lock for mdid
	pmdaccelem->Pmdid()->RemoveDeletionLock();;

	GPOS_DELETE(pmdaccelem);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::DestroyProviderElement
//
//	@doc:
//		Destroy provider element;
//		called only at destruction time
//
//---------------------------------------------------------------------------
void
CMDAccessor::DestroyProviderElement
	(
	SMDProviderElem *pmdpelem
	)
{
	GPOS_DELETE(pmdpelem);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::InitHashtables
//
//	@doc:
//		Initializes the hash tables
//
//---------------------------------------------------------------------------
void
CMDAccessor::InitHashtables
	(
	IMemoryPool *pmp
	)
{
	// initialize Cache accessors hash table
	m_shtCacheAccessors.Init
				(
				pmp,
				GPOPT_CACHEACC_HT_NUM_OF_BUCKETS,
				GPOS_OFFSET(SMDAccessorElem, m_link),
				GPOS_OFFSET(SMDAccessorElem, m_pmdid),
				&(SMDAccessorElem::m_pmdidInvalid),
				SMDAccessorElem::UlHash,
				SMDAccessorElem::FEqual
				);
	
	// initialize MD providers hash table
	m_shtProviders.Init
		(
		pmp,
		GPOPT_CACHEACC_HT_NUM_OF_BUCKETS,
		GPOS_OFFSET(SMDProviderElem, m_link),
		0, // the HT element is used as key
		&(SMDProviderElem::m_mdpelemInvalid),
		SMDProviderElem::UlHash,
		SMDProviderElem::FEqual
		);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::~CMDAccessor
//
//	@doc:
//		Destructor 
//
//---------------------------------------------------------------------------
CMDAccessor::~CMDAccessor()
{
	// release cache accessors and MD providers in hashtables
	m_shtCacheAccessors.DestroyEntries(DestroyAccessorElement);
	m_shtProviders.DestroyEntries(DestroyProviderElement);
	GPOS_DELETE(m_pmdpGeneric);

	if (GPOS_FTRACE(EopttracePrintOptimizationStatistics))
	{
		// print fetch time and lookup time
		CAutoTrace at(m_pmp);
		at.Os() << "[OPT]: Total metadata fetch time: " << m_dFetchTime << "ms" << std::endl;
		at.Os() << "[OPT]: Total metadata lookup time (including fetch time): " << m_dLookupTime << "ms" << std::endl;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::RegisterProvider
//
//	@doc:
//		Register a MD provider for the given source system id
//
//---------------------------------------------------------------------------
void
CMDAccessor::RegisterProvider
	(
	CSystemId sysid,
	IMDProvider *pmdp
	)
{	
	CAutoP<SMDProviderElem> a_pmdpelem;
	a_pmdpelem = GPOS_NEW(m_pmp) SMDProviderElem(sysid, pmdp);
	
	MDPHTAccessor mdhtacc(m_shtProviders, *(a_pmdpelem.Pt()));

	GPOS_ASSERT(NULL == mdhtacc.PtLookup());
	
	// insert provider in the hash table
	mdhtacc.Insert(a_pmdpelem.Pt());
	a_pmdpelem.PtReset();
}


//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::RegisterProviders
//
//	@doc:
//		Register given MD providers
//
//---------------------------------------------------------------------------
void
CMDAccessor::RegisterProviders
	(
	const DrgPsysid *pdrgpsysid,
	const DrgPmdp *pdrgpmdp
	)
{
	GPOS_ASSERT(NULL != pdrgpmdp);
	GPOS_ASSERT(NULL != pdrgpsysid);
	GPOS_ASSERT(pdrgpmdp->UlLength() == pdrgpsysid->UlLength());
	GPOS_ASSERT(0 < pdrgpmdp->UlLength());

	const ULONG ulProviders = pdrgpmdp->UlLength();
	for (ULONG ul = 0; ul < ulProviders; ul++)
	{
		IMDProvider *pmdp = (*pdrgpmdp)[ul];
		pmdp->AddRef();
		RegisterProvider(*((*pdrgpsysid)[ul]), pmdp);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::Pmdp
//
//	@doc:
//		Retrieve the MD provider for the given source system id
//
//---------------------------------------------------------------------------
IMDProvider *
CMDAccessor::Pmdp
	(
	CSystemId sysid
	)
{	
	SMDProviderElem *pmdpelem = NULL;
	
	{
		// scope for HT accessor
		
		SMDProviderElem mdpelem(sysid, NULL /*pmdp*/);
		MDPHTAccessor mdhtacc(m_shtProviders, mdpelem);
		
		pmdpelem = mdhtacc.PtLookup();
	}
	
	GPOS_ASSERT(NULL != pmdpelem && "Could not find MD provider");
	
	return pmdpelem->Pmdp();
}



//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::Pimdobj
//
//	@doc:
//		Retrieves a metadata cache object from the md cache, possibly retrieving
//		it from the external metadata provider and storing it in the cache first.
//		Main workhorse for retrieving the different types of md cache objects.
//
//---------------------------------------------------------------------------
const IMDCacheObject *
CMDAccessor::Pimdobj
	(
	IMDId *pmdid
	)
{
	CTimerUser timerLookup; // timer to measure lookup time

	const IMDCacheObject *pimdobj = NULL;

	// first, try to locate object in local hashtable
	{
		// scope for ht accessor
		MDHTAccessor mdhtacc(m_shtCacheAccessors, pmdid);
		SMDAccessorElem *pmdaccelem = mdhtacc.PtLookup(); 
		if (NULL != pmdaccelem)
		{
			pimdobj = pmdaccelem->Pimdobj();
		}
	}

	if (NULL == pimdobj)
	{
		// object not in local hashtable, try lookup in the MD cache
		
		// construct a key for cache lookup
		IMDProvider *pmdp = Pmdp(pmdid->Sysid());
		
		CMDKey mdkey(pmdid);
				
		CAutoP<CacheAccessorMD> a_pmdcacc;
		a_pmdcacc = GPOS_NEW(m_pmp) CacheAccessorMD(m_pcache);
		a_pmdcacc->Lookup(&mdkey);
		IMDCacheObject *pmdobjNew = a_pmdcacc->PtVal();
		if (NULL == pmdobjNew)
		{
			// object not found in MD cache: retrieve it from MD provider
			CTimerUser timerFetch;  // timer to measure fetch time
			CAutoP<CWStringBase> a_pstr;
			a_pstr = pmdp->PstrObject(m_pmp, this, pmdid);
			
			GPOS_ASSERT(NULL != a_pstr.Pt());
			IMemoryPool *pmp = m_pmp;
			
			if (IMDId::EmdidGPDBCtas != pmdid->Emdidt())
			{
				// create the accessor memory pool
				pmp = a_pmdcacc->Pmp();
			}

			pmdobjNew = gpdxl::CDXLUtils::PimdobjParseDXL(pmp, a_pstr.Pt(), NULL /* XSD path */);
			GPOS_ASSERT(NULL != pmdobjNew);

			if (GPOS_FTRACE(EopttracePrintOptimizationStatistics))
			{
				// add fetch time in msec
				CDouble dFetch(timerFetch.UlElapsedUS() / CDouble(GPOS_USEC_IN_MSEC));
				m_dFetchTime = CDouble(m_dFetchTime.DVal() + dFetch.DVal());
			}

			// For CTAS mdid, we avoid adding the corresponding object to the MD cache
			// since those objects have a fixed id, and if caching is enabled and those
			// objects are cached, then a subsequent CTAS query will attempt to use
			// the cached object, which has a different schema, resulting in a crash.
			// so for such objects, we bypass the MD cache, getting them from the
			// MD provider, directly to the local hash table

			if (IMDId::EmdidGPDBCtas != pmdid->Emdidt())
			{
				// add to MD cache
				CAutoP<CMDKey> a_pmdkeyCache;
				// ref count of the new object is set to one and optimizer becomes its owner
				a_pmdkeyCache = GPOS_NEW(pmp) CMDKey(pmdobjNew->Pmdid());

				// object gets pinned independent of whether insertion succeeded or
				// failed because object was already in cache

#ifdef GPOS_DEBUG
				IMDCacheObject *pmdobjInserted =
#endif
				a_pmdcacc->PtInsert(a_pmdkeyCache.Pt(), pmdobjNew);

				GPOS_ASSERT(NULL != pmdobjInserted);

				// safely inserted
				(void) a_pmdkeyCache.PtReset();
			}
		}

		{
			// store in local hashtable
			GPOS_ASSERT(NULL != pmdobjNew);
			IMDId *pmdidNew = pmdobjNew->Pmdid();
			pmdidNew->AddRef();

			CAutoP<SMDAccessorElem> a_pmdaccelem;
			a_pmdaccelem = GPOS_NEW(m_pmp) SMDAccessorElem(pmdobjNew, pmdidNew);

			MDHTAccessor mdhtacc(m_shtCacheAccessors, a_pmdaccelem->Pmdid());

			if (NULL == mdhtacc.PtLookup())
			{
				// object has not been inserted in the meantime
				mdhtacc.Insert(a_pmdaccelem.Pt());

				// add deletion lock for mdid
				a_pmdaccelem->Pmdid()->AddDeletionLock();
				a_pmdaccelem.PtReset();
			}
		}
	}
	
	// requested object must be in local hashtable already: retrieve it
	MDHTAccessor mdhtacc(m_shtCacheAccessors, pmdid);
	SMDAccessorElem *pmdaccelem = mdhtacc.PtLookup();
	
	GPOS_ASSERT(NULL != pmdaccelem);
	
	pimdobj = pmdaccelem->Pimdobj();
	GPOS_ASSERT(NULL != pimdobj);
	
	if (GPOS_FTRACE(EopttracePrintOptimizationStatistics))
	{
		// add lookup time in msec
		CDouble dLookup(timerLookup.UlElapsedUS() / CDouble(GPOS_USEC_IN_MSEC));
		m_dLookupTime = CDouble(m_dLookupTime.DVal() + dLookup.DVal());
	}

	return pimdobj;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::Pmdrel
//
//	@doc:
//		Retrieves a metadata cache relation from the md cache, possibly retrieving
//		it from the external metadata provider and storing it in the cache first.
//
//---------------------------------------------------------------------------
const IMDRelation *
CMDAccessor::Pmdrel
	(
	IMDId *pmdid
	)
{
	const IMDCacheObject *pmdobj = Pimdobj(pmdid);
	if (IMDCacheObject::EmdtRel != pmdobj->Emdt())
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, pmdid->Wsz());
	}

	return dynamic_cast<const IMDRelation*>(pmdobj);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::Pmdtype
//
//	@doc:
//		Retrieves the metadata description for a type from the md cache, 
//		possibly retrieving it from the external metadata provider and storing 
//		it in the cache first.
//
//---------------------------------------------------------------------------
const IMDType *
CMDAccessor::Pmdtype
	(
	IMDId *pmdid
	)
{	
	const IMDCacheObject *pmdobj = Pimdobj(pmdid);
	if (IMDCacheObject::EmdtType != pmdobj->Emdt())
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, pmdid->Wsz());
	}

	return dynamic_cast<const IMDType*>(pmdobj);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::Pmdtype
//
//	@doc:
//		Retrieves the MD type from the md cache given the type info and source
//		system id,  possibly retrieving it from the external metadata provider 
//		and storing it in the cache first.
//
//---------------------------------------------------------------------------
const IMDType *
CMDAccessor::Pmdtype
	(
	CSystemId sysid,
	IMDType::ETypeInfo eti
	)
{	
	GPOS_ASSERT(IMDType::EtiGeneric != eti);
	IMDProvider *pmdp = Pmdp(sysid);
	CAutoRef<IMDId> a_pmdid;
	a_pmdid = pmdp->Pmdid(m_pmp, sysid, eti);
	const IMDCacheObject *pmdobj = Pimdobj(a_pmdid.Pt());
	if (IMDCacheObject::EmdtType != pmdobj->Emdt())
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, a_pmdid.Pt()->Wsz());
	}

	return dynamic_cast<const IMDType*>(pmdobj);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::Pmdtype
//
//	@doc:
//		Retrieves the generic MD type from the md cache given the
//		type info,  possibly retrieving it from the external metadata provider 
//		and storing it in the cache first.
//
//---------------------------------------------------------------------------
const IMDType *
CMDAccessor::Pmdtype
	(
	IMDType::ETypeInfo eti
	)
{	
	GPOS_ASSERT(IMDType::EtiGeneric != eti);

	IMDId *pmdid = m_pmdpGeneric->Pmdid(eti);
	GPOS_ASSERT(NULL != pmdid);
	const IMDCacheObject *pmdobj = Pimdobj(pmdid);
	
	if (IMDCacheObject::EmdtType != pmdobj->Emdt())
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, pmdid->Wsz());
	}

	return dynamic_cast<const IMDType*>(pmdobj);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::Pmdscop
//
//	@doc:
//		Retrieves the metadata description for a scalar operator from the md cache, 
//		possibly retrieving it from the external metadata provider and storing 
//		it in the cache first.
//
//---------------------------------------------------------------------------
const IMDScalarOp *
CMDAccessor::Pmdscop
	(
	IMDId *pmdid
	)
{	
	const IMDCacheObject *pmdobj = Pimdobj(pmdid);
	if (IMDCacheObject::EmdtOp != pmdobj->Emdt())
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, pmdid->Wsz());
	}

	return dynamic_cast<const IMDScalarOp*>(pmdobj);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::Pmdfunc
//
//	@doc:
//		Retrieves the metadata description for a function from the md cache, 
//		possibly retrieving it from the external metadata provider and storing 
//		it in the cache first.
//
//---------------------------------------------------------------------------
const IMDFunction *
CMDAccessor::Pmdfunc
	(
	IMDId *pmdid
	)
{	
	const IMDCacheObject *pmdobj = Pimdobj(pmdid);
	if (IMDCacheObject::EmdtFunc != pmdobj->Emdt())
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, pmdid->Wsz());
	}

	return dynamic_cast<const IMDFunction*>(pmdobj);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::FaggWindowFunc
//
//	@doc:
//		Check if the retrieved the window function metadata description from 
//		the md cache is an aggregate window function. Internally this function
//		may retrieve it from the external metadata provider and storing
//		it in the cache.
//
//---------------------------------------------------------------------------
BOOL
CMDAccessor::FAggWindowFunc
	(
	IMDId *pmdid
	)
{
	const IMDCacheObject *pmdobj = Pimdobj(pmdid);

	return (IMDCacheObject::EmdtAgg == pmdobj->Emdt());
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::Pmdagg
//
//	@doc:
//		Retrieves the metadata description for an aggregate from the md cache, 
//		possibly retrieving it from the external metadata provider and storing 
//		it in the cache first.
//
//---------------------------------------------------------------------------
const IMDAggregate *
CMDAccessor::Pmdagg
	(
	IMDId *pmdid
	)
{	
	const IMDCacheObject *pmdobj = Pimdobj(pmdid);
	if (IMDCacheObject::EmdtAgg != pmdobj->Emdt())
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, pmdid->Wsz());
	}

	return dynamic_cast<const IMDAggregate*>(pmdobj);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::Pmdtrigger
//
//	@doc:
//		Retrieves the metadata description for a trigger from the md cache,
//		possibly retrieving it from the external metadata provider and storing
//		it in the cache first.
//
//---------------------------------------------------------------------------
const IMDTrigger *
CMDAccessor::Pmdtrigger
	(
	IMDId *pmdid
	)
{
	const IMDCacheObject *pmdobj = Pimdobj(pmdid);
	if (IMDCacheObject::EmdtTrigger != pmdobj->Emdt())
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, pmdid->Wsz());
	}

	return dynamic_cast<const IMDTrigger*>(pmdobj);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::Pmdindex
//
//	@doc:
//		Retrieves the metadata description for an index from the md cache, 
//		possibly retrieving it from the external metadata provider and storing 
//		it in the cache first.
//
//---------------------------------------------------------------------------
const IMDIndex *
CMDAccessor::Pmdindex
	(
	IMDId *pmdid
	)
{	
	const IMDCacheObject *pmdobj = Pimdobj(pmdid);
	if (IMDCacheObject::EmdtInd != pmdobj->Emdt())
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, pmdid->Wsz());
	}

	return dynamic_cast<const IMDIndex*>(pmdobj);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::Pmdcheckconstraint
//
//	@doc:
//		Retrieves the metadata description for a check constraint from the md cache,
//		possibly retrieving it from the external metadata provider and storing
//		it in the cache first.
//
//---------------------------------------------------------------------------
const IMDCheckConstraint *
CMDAccessor::Pmdcheckconstraint
	(
	IMDId *pmdid
	)
{
	const IMDCacheObject *pmdobj = Pimdobj(pmdid);
	if (IMDCacheObject::EmdtCheckConstraint != pmdobj->Emdt())
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, pmdid->Wsz());
	}

	return dynamic_cast<const IMDCheckConstraint*>(pmdobj);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::Pmdcolstats
//
//	@doc:
//		Retrieves column statistics from the md cache, possibly retrieving it  
//		from the external metadata provider and storing it in the cache first.
//
//---------------------------------------------------------------------------
const IMDColStats *
CMDAccessor::Pmdcolstats
	(
	IMDId *pmdid
	)
{	
	const IMDCacheObject *pmdobj = Pimdobj(pmdid);
	if (IMDCacheObject::EmdtColStats != pmdobj->Emdt())
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, pmdid->Wsz());
	}

	return dynamic_cast<const IMDColStats*>(pmdobj);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::Pmdrelstats
//
//	@doc:
//		Retrieves relation statistics from the md cache, possibly retrieving it  
//		from the external metadata provider and storing it in the cache first.
//
//---------------------------------------------------------------------------
const IMDRelStats *
CMDAccessor::Pmdrelstats
	(
	IMDId *pmdid
	)
{	
	const IMDCacheObject *pmdobj = Pimdobj(pmdid);
	if (IMDCacheObject::EmdtRelStats != pmdobj->Emdt())
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, pmdid->Wsz());
	}

	return dynamic_cast<const IMDRelStats*>(pmdobj);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::Pmdcast
//
//	@doc:
//		Retrieve cast object between given source and destination types
//
//---------------------------------------------------------------------------
const IMDCast *
CMDAccessor::Pmdcast
	(
	IMDId *pmdidSrc,
	IMDId *pmdidDest
	)
{	
	GPOS_ASSERT(NULL != pmdidSrc);
	GPOS_ASSERT(NULL != pmdidDest);
	
	pmdidSrc->AddRef();
	pmdidDest->AddRef();
	
	CAutoP<IMDId> a_pmdidCast;
	a_pmdidCast = GPOS_NEW(m_pmp) CMDIdCast(CMDIdGPDB::PmdidConvert(pmdidSrc), CMDIdGPDB::PmdidConvert(pmdidDest));
	
	const IMDCacheObject *pmdobj = Pimdobj(a_pmdidCast.Pt());
		
	if (IMDCacheObject::EmdtCastFunc != pmdobj->Emdt())
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, a_pmdidCast->Wsz());
	}
	a_pmdidCast.PtReset()->Release();

	return dynamic_cast<const IMDCast*>(pmdobj);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::Pmdsccmp
//
//	@doc:
//		Retrieve scalar comparison object between given types
//
//---------------------------------------------------------------------------
const IMDScCmp *
CMDAccessor::Pmdsccmp
	(
	IMDId *pmdidLeft,
	IMDId *pmdidRight,
	IMDType::ECmpType ecmpt
	)
{	
	GPOS_ASSERT(NULL != pmdidLeft);
	GPOS_ASSERT(NULL != pmdidLeft);
	GPOS_ASSERT(IMDType::EcmptOther > ecmpt);
	
	pmdidLeft->AddRef();
	pmdidRight->AddRef();
	
	CAutoP<IMDId> a_pmdidScCmp;
	a_pmdidScCmp = GPOS_NEW(m_pmp) CMDIdScCmp(CMDIdGPDB::PmdidConvert(pmdidLeft), CMDIdGPDB::PmdidConvert(pmdidRight), ecmpt);
	
	const IMDCacheObject *pmdobj = Pimdobj(a_pmdidScCmp.Pt());
		
	if (IMDCacheObject::EmdtScCmp != pmdobj->Emdt())
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, a_pmdidScCmp->Wsz());
	}
	a_pmdidScCmp.PtReset()->Release();

	return dynamic_cast<const IMDScCmp*>(pmdobj);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::ExtractColumnHistWidth
//
//	@doc:
//		Record histogram and width information for a given column of a table
//
//---------------------------------------------------------------------------
void
CMDAccessor::RecordColumnStats
	(
	IMemoryPool *pmp,
	IMDId *pmdidRel,
	ULONG ulColId,
	ULONG ulPos,
	BOOL fSystemCol,
	BOOL fEmptyTable,
	HMUlHist *phmulhist,
	HMUlDouble *phmuldoubleWidth,
	CStatisticsConfig *pstatsconf
	)
{
	GPOS_ASSERT(NULL != pmdidRel);
	GPOS_ASSERT(NULL != phmulhist);
	GPOS_ASSERT(NULL != phmuldoubleWidth);

	// get the column statistics
	const IMDColStats *pmdcolstats = Pmdcolstats(pmp, pmdidRel, ulPos);
	GPOS_ASSERT(NULL != pmdcolstats);

	// fetch the column width and insert it into the hashmap
	CDouble *pdWidth = GPOS_NEW(pmp) CDouble(pmdcolstats->DWidth());
	phmuldoubleWidth->FInsert(GPOS_NEW(pmp) ULONG(ulColId), pdWidth);

	// extract the the histogram and insert it into the hashmap
	const IMDRelation *pmdrel = Pmdrel(pmdidRel);
	IMDId *pmdidType = pmdrel->Pmdcol(ulPos)->PmdidType();
	CHistogram *phist = Phist(pmp, pmdidType, pmdcolstats);
	GPOS_ASSERT(NULL != phist);
	phmulhist->FInsert(GPOS_NEW(pmp) ULONG(ulColId), phist);

	BOOL fGuc = GPOS_FTRACE(EopttracePrintColsWithMissingStats);
	BOOL fRecordMissingStats = !fEmptyTable && fGuc && !fSystemCol
								&& (NULL != pstatsconf) && phist->FColStatsMissing();
	if (fRecordMissingStats)
	{
		// record the columns with missing (dummy) statistics information
		pmdidRel->AddRef();
		CMDIdColStats *pmdidCol = GPOS_NEW(pmp) CMDIdColStats
												(
												CMDIdGPDB::PmdidConvert(pmdidRel),
												ulPos
												);
		pstatsconf->AddMissingStatsColumn(pmdidCol);
		pmdidCol->Release();
	}
}


// Return the column statistics meta data object for a given column of a table
const IMDColStats *
CMDAccessor::Pmdcolstats
	(
	IMemoryPool *pmp,
	IMDId *pmdidRel,
	ULONG ulPos
	)
{
	pmdidRel->AddRef();
	CMDIdColStats *pmdidColStats = GPOS_NEW(pmp) CMDIdColStats(CMDIdGPDB::PmdidConvert(pmdidRel), ulPos);
	const IMDColStats *pmdcolstats = Pmdcolstats(pmdidColStats);
	pmdidColStats->Release();

	return pmdcolstats;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::Pstats
//
//	@doc:
//		Construct a statistics object for the columns of the given relation
//
//---------------------------------------------------------------------------
IStatistics *
CMDAccessor::Pstats
	(
	IMemoryPool *pmp,
	IMDId *pmdidRel,
	CColRefSet *pcrsHist,
	CColRefSet *pcrsWidth,
	CStatisticsConfig *pstatsconf
	)
{
	GPOS_ASSERT(NULL != pmdidRel);
	GPOS_ASSERT(NULL != pcrsHist);
	GPOS_ASSERT(NULL != pcrsWidth);

	// retrieve MD relation and MD relation stats objects
	pmdidRel->AddRef();
	CMDIdRelStats *pmdidRelStats = GPOS_NEW(pmp) CMDIdRelStats(CMDIdGPDB::PmdidConvert(pmdidRel));
	const IMDRelStats *pmdRelStats = Pmdrelstats(pmdidRelStats);
	pmdidRelStats->Release();

	BOOL fEmptyTable = pmdRelStats->FEmpty();
	const IMDRelation *pmdrel = Pmdrel(pmdidRel);

	HMUlHist *phmulhist = GPOS_NEW(pmp) HMUlHist(pmp);
	HMUlDouble *phmuldoubleWidth = GPOS_NEW(pmp) HMUlDouble(pmp);

	CColRefSetIter crsiHist(*pcrsHist);
	while (crsiHist.FAdvance())
	{
		CColRef *pcrHist = crsiHist.Pcr();

		// colref must be one of the base table
		CColRefTable *pcrtable = CColRefTable::PcrConvert(pcrHist);

		// extract the column identifier, position of the attribute in the system catalog
		ULONG ulColId = pcrtable->UlId();
		INT iAttno = pcrtable->IAttno();
		ULONG ulPos = pmdrel->UlPosFromAttno(iAttno);

		RecordColumnStats
			(
			pmp,
			pmdidRel,
			ulColId,
			ulPos,
			pcrtable->FSystemCol(),
			fEmptyTable,
			phmulhist,
			phmuldoubleWidth,
			pstatsconf
			);
	}

	// extract column widths
	CColRefSetIter crsiWidth(*pcrsWidth);

	while (crsiWidth.FAdvance())
	{
		CColRef *pcrWidth = crsiWidth.Pcr();

		// colref must be one of the base table
		CColRefTable *pcrtable = CColRefTable::PcrConvert(pcrWidth);

		// extract the column identifier, position of the attribute in the system catalog
		ULONG ulColId = pcrtable->UlId();
		INT iAttno = pcrtable->IAttno();
		ULONG ulPos = pmdrel->UlPosFromAttno(iAttno);

		CDouble *pdWidth = GPOS_NEW(pmp) CDouble(pmdrel->DColWidth(ulPos));
		phmuldoubleWidth->FInsert(GPOS_NEW(pmp) ULONG(ulColId), pdWidth);
	}

	CDouble dRows = std::max(DOUBLE(1.0), pmdRelStats->DRows().DVal());

	return GPOS_NEW(pmp) CStatistics
							(
							pmp,
							phmulhist,
							phmuldoubleWidth,
							dRows,
							fEmptyTable
							);
}


//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::Phist
//
//	@doc:
//		Construct a histogram from the given MD column stats object
//
//---------------------------------------------------------------------------
CHistogram *
CMDAccessor::Phist
	(
	IMemoryPool *pmp,
	IMDId *pmdidType,
	const IMDColStats *pmdcolstats
	)
{
	GPOS_ASSERT(NULL != pmdidType);
	GPOS_ASSERT(NULL != pmdcolstats);

	BOOL fColStatsMissing = pmdcolstats->FColStatsMissing();
	const ULONG ulBuckets = pmdcolstats->UlBuckets();
	BOOL fBoolType = CMDAccessorUtils::FBoolType(this, pmdidType);
	if (fColStatsMissing && fBoolType)
	{
		GPOS_ASSERT(0 == ulBuckets);

		return CHistogram::PhistDefaultBoolColStats(pmp);
	}

	DrgPbucket *pdrgpbucket = GPOS_NEW(pmp) DrgPbucket(pmp);
	for (ULONG ul = 0; ul < ulBuckets; ul++)
	{
		const CDXLBucket *pdxlbucket = pmdcolstats->Pdxlbucket(ul);
		CBucket *pbucket = Pbucket(pmp, pmdidType, pdxlbucket);
		pdrgpbucket->Append(pbucket);
	}

	CDouble dNullFreq = pmdcolstats->DNullFreq();
	CDouble dDistinctRemain = pmdcolstats->DDistinctRemain();
	CDouble dFreqRemain = pmdcolstats->DFreqRemain();

	CHistogram *phist = GPOS_NEW(pmp) CHistogram
									(
									pdrgpbucket,
									true /*fWellDefined*/,
									dNullFreq,
									dDistinctRemain,
									dFreqRemain,
									fColStatsMissing
									);
	GPOS_ASSERT_IMP(fBoolType, 3 >= phist->DDistinct() - CStatistics::DEpsilon);

	return phist;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::Pbucket
//
//	@doc:
//		Construct a typed bucket from a DXL bucket
//
//---------------------------------------------------------------------------
CBucket *
CMDAccessor::Pbucket
	(
	IMemoryPool *pmp,
	IMDId *pmdidType,
	const CDXLBucket *pdxlbucket
	)
{
	IDatum *pdatumLower = Pdatum(pmp, pmdidType, pdxlbucket->PdxldatumLower());
	IDatum *pdatumUpper = Pdatum(pmp, pmdidType, pdxlbucket->PdxldatumUpper());
	
	CPoint *ppointLower = GPOS_NEW(pmp) CPoint(pdatumLower);
	CPoint *ppointUpper = GPOS_NEW(pmp) CPoint(pdatumUpper);
	
	return GPOS_NEW(pmp) CBucket
						(
						ppointLower,
						ppointUpper,
						pdxlbucket->FLowerClosed(),
						pdxlbucket->FUpperClosed(),
						pdxlbucket->DFrequency(),
						pdxlbucket->DDistinct()
						);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::Pdatum
//
//	@doc:
//		Construct a typed bucket from a DXL bucket
//
//---------------------------------------------------------------------------
IDatum *
CMDAccessor::Pdatum
	(
	IMemoryPool *pmp,
	IMDId *pmdidType,
	const CDXLDatum *pdxldatum
	)
{
	const IMDType *pmdtype = Pmdtype(pmdidType);
		
	return pmdtype->Pdatum(pmp, pdxldatum);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::Serialize
//
//	@doc:
//		Serialize MD object into provided stream
//
//---------------------------------------------------------------------------
void
CMDAccessor::Serialize
	(
	COstream& oos
	)
{
	ULONG nentries = m_shtCacheAccessors.UlpEntries();
	IMDCacheObject **cacheEntries;
	CAutoRg<IMDCacheObject*> aCacheEntries;
	ULONG ul;

	// Iterate the hash table and insert all entries to the array.
	// The iterator holds a lock on the hash table, so we must not
	// do anything non-trivial that might e.g. allocate memory,
	// while iterating.
	cacheEntries = GPOS_NEW_ARRAY(m_pmp, IMDCacheObject *, nentries);
	aCacheEntries = cacheEntries;
	{
		MDHTIter mdhtit(m_shtCacheAccessors);
		ul = 0;
		while (mdhtit.FAdvance())
		{
			MDHTIterAccessor mdhtitacc(mdhtit);
			SMDAccessorElem *pmdaccelem = mdhtitacc.Pt();
			GPOS_ASSERT(NULL != pmdaccelem);
			cacheEntries[ul++] = pmdaccelem->Pimdobj();
		}
		GPOS_ASSERT(ul == nentries);
	}

	// Now that we're done iterating and no longer hold the lock,
	// serialize the entries.
	for (ul = 0; ul < nentries; ul++)
		oos << cacheEntries[ul]->Pstr()->Wsz();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::SerializeSysid
//
//	@doc:
//		Serialize the system ids into provided stream
//
//---------------------------------------------------------------------------
void
CMDAccessor::SerializeSysid
	(
	COstream &oos
	)
{
	ULONG ul = 0;
	MDPHTIter mdhtit(m_shtProviders);

	while (mdhtit.FAdvance())
	{
		MDPHTIterAccessor mdhtitacc(mdhtit);
		SMDProviderElem *pmdpelem = mdhtitacc.Pt();
		CSystemId sysid = pmdpelem->Sysid();
		
		
		WCHAR wszSysId[GPDXL_MDID_LENGTH];
		CWStringStatic str(wszSysId, GPOS_ARRAY_SIZE(wszSysId));
		
		if (0 < ul)
		{
			str.AppendFormat(GPOS_WSZ_LIT("%s"), ",");
		}
		
		str.AppendFormat(GPOS_WSZ_LIT("%d.%ls"), sysid.Emdidt(), sysid.Wsz());

		oos << str.Wsz();
		ul++;
	}
}


// EOF
