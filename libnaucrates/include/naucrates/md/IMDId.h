//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		IMDId.h
//
//	@doc:
//		Abstract class for representing metadata object ids
//---------------------------------------------------------------------------



#ifndef GPMD_IMDId_H
#define GPMD_IMDId_H

#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/common/CHashSet.h"
#include "gpos/common/CHashSetIter.h"
#include "gpos/string/CWStringConst.h"

#include "naucrates/dxl/gpdb_types.h"

#define GPDXL_MDID_LENGTH 100

// fwd decl
namespace gpdxl
{
	class CXMLSerializer;
}

namespace gpmd
{
	using namespace gpos;
	using namespace gpdxl;

	// fwd decl
	class CSystemId;

	static const INT IDefaultTypeModifier = -1;
	static const OID OidInvalidCollation = 0;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		IMDId
	//
	//	@doc:
	//		Abstract class for representing metadata objects ids
	//
	//---------------------------------------------------------------------------
	class IMDId : public CRefCount
	{
		
		private:

			// number of deletion locks -- each MDAccessor adds a new deletion lock if it uses
			// an MDId object in its internal hash-table, the deletion lock is released when
			// MDAccessor is destroyed
			volatile ULONG_PTR m_ulpDeletionLocks;

		public:
			//------------------------------------------------------------------
			//	@doc:
			//		Type of md id.
			//		The exact values are important when parsing mdids from DXL and
			//		should not be modified without modifying the parser
			//
			//------------------------------------------------------------------
			enum EMDIdType
			{
				EmdidGPDB = 0,
				EmdidColStats = 1,
				EmdidRelStats = 2,
				EmdidCastFunc = 3,
				EmdidScCmp = 4,
				EmdidGPDBCtas = 5,
				EmdidSentinel
			};
			
			// ctor
			IMDId()
				:
				m_ulpDeletionLocks(0)
			{}

			// dtor
			virtual 
			~IMDId(){};
			
			// type of mdid
			virtual
			EMDIdType Emdidt() const = 0;
			
			// string representation of mdid
			virtual
			const WCHAR *Wsz() const = 0;
			
			// system id
			virtual
			CSystemId Sysid() const = 0;
			
			
			// equality check
			virtual 
			BOOL FEquals(const IMDId *pmdid) const = 0;
					
			// computes the hash value for the metadata id
			virtual
			ULONG UlHash() const = 0;
			
			// return true if calling object's destructor is allowed
			virtual
			BOOL FDeletable() const
			{
				return (0 == m_ulpDeletionLocks);
			}

			// increase number of deletion locks
			void AddDeletionLock()
			{
				(void) UlpExchangeAdd(&m_ulpDeletionLocks, 1);
			}

			// decrease number of deletion locks
			void RemoveDeletionLock()
			{
				GPOS_ASSERT(0 < m_ulpDeletionLocks);

				(void) UlpExchangeAdd(&m_ulpDeletionLocks, -1);
			}

			// return number of deletion locks
			ULONG_PTR UlpDeletionLocks() const
			{
				return m_ulpDeletionLocks;
			}

			// static hash functions for use in different indexing structures,
			// e.g. hashmaps, MD cache, etc.
			static
			ULONG UlHashMDId
				(
				const IMDId *pmdid
				)
			{
				GPOS_ASSERT(NULL != pmdid);
				return pmdid->UlHash();
			}

			// hash function for using mdids in a cache
			static
			ULONG UlHashMDid
				(
				const VOID_PTR & pv
				)
			{
				GPOS_ASSERT(NULL != pv);
				IMDId *pmdid = static_cast<IMDId *> (pv);
				return pmdid->UlHash();
			}
			
			// static equality functions for use in different structures, 
			// e.g. hashmaps, MD cache, etc.
			static 
			BOOL FEqualMDId
				(
				const IMDId *pmdidLeft,
				const IMDId *pmdidRight
				)
			{
				GPOS_ASSERT(NULL != pmdidLeft && NULL != pmdidRight);
				return pmdidLeft->FEquals(pmdidRight);
			}
			
			// equality function for using mdids in a cache
			static BOOL
			FEqualMDid
				(
				const VOID_PTR &pvLeft,
				const VOID_PTR &pvRight
				)
			{
				if (NULL == pvLeft && NULL == pvRight)
				{
					return true;
				}
				
				if (NULL == pvLeft || NULL == pvRight)
				{
					return false;
				}
				
			
				IMDId *pmdidLeft = static_cast<IMDId *> (pvLeft);
				IMDId *pmdidRight = static_cast<IMDId *> (pvRight);
				return pmdidLeft->FEquals(pmdidRight);

			}
			
			// is the mdid valid
			virtual
			BOOL FValid() const = 0;
			
			// serialize mdid in DXL as the value for the specified attribute 
			virtual
			void Serialize(CXMLSerializer *pxmlser, const CWStringConst *pstrAttribute) const = 0;

			// debug print of the metadata id
			virtual
			IOstream &OsPrint(IOstream &os) const = 0;
			
			// safe validity function
			static
			BOOL FValid(const IMDId *pmdid)
			{
				return NULL != pmdid && pmdid->FValid();
			}
	};
	
	// common structures over metadata id elements
	typedef CDynamicPtrArray<IMDId, CleanupRelease> DrgPmdid;

    // hash set for mdid
    typedef CHashSet<IMDId, IMDId::UlHashMDId, IMDId::FEqualMDId, CleanupRelease<IMDId> > HSMDId;

    // iterator over the hash set for column id information for missing statistics
    typedef CHashSetIter<IMDId, IMDId::UlHashMDId, IMDId::FEqualMDId, CleanupRelease<IMDId> > HSIterMDId;
}



#endif // !GPMD_IMDId_H

// EOF
