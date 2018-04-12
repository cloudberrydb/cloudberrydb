//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLColRef.h
//
//	@doc:
//		Class for representing column references.
//---------------------------------------------------------------------------



#ifndef GPDXL_CDXLColRef_H
#define GPDXL_CDXLColRef_H

#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/common/CRefCount.h"

#include "naucrates/md/CMDName.h"
#include "naucrates/md/IMDId.h"

namespace gpdxl
{
	using namespace gpmd;
	using namespace gpos;

	// fwd decl
	class CXMLSerializer;
	class CDXLColRef;
	
	// arrays of column references
	typedef CDynamicPtrArray<CDXLColRef, CleanupRelease> DrgPdxlcr;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLColRef
	//
	//	@doc:
	//		Class for representing references to columns in DXL trees
	//
	//---------------------------------------------------------------------------
	class CDXLColRef : public CRefCount
	{
		private:
			// memory pool
			IMemoryPool *m_pmp;
			
			// name
			CMDName *m_pmdname;
	
			// id
			const ULONG m_ulId;

			// column type
			IMDId *m_pmdidType;

			// column type modifier
			INT m_iTypeModifer;

			OID m_oidCollation;

			// private copy ctor
			CDXLColRef(const CDXLColRef &);
		
		public:
			// ctor
			CDXLColRef
				(
				IMemoryPool *pmp,
				CMDName *pmdname,
				ULONG ulId,
				IMDId *pmdidType,
				INT iTypeModifier,
				OID oidCollation
				);

			// ctor for invalid collation
			CDXLColRef
				(
				IMemoryPool *pmp,
				CMDName *pmdname,
				ULONG ulId,
				IMDId *pmdidType,
				INT iTypeModifier
				);

			// dtor
			~CDXLColRef();
			
			// accessors
			const CMDName *Pmdname() const;

			IMDId *PmdidType() const;

			INT ITypeModifier() const;

			OID OidCollation() const;

			ULONG UlID() const;

	};
}



#endif // !GPDXL_CDXLColRef_H

// EOF

