//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLColDescr.h
//
//	@doc:
//		Class for representing column descriptors.
//---------------------------------------------------------------------------



#ifndef GPDXL_CDXLColDescr_H
#define GPDXL_CDXLColDescr_H

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"

#include "naucrates/md/CMDName.h"
#include "naucrates/md/CMDIdGPDB.h"

namespace gpdxl
{

	using namespace gpmd;
	
	// fwd decl
	class CXMLSerializer;
	class CDXLColDescr;
	
	typedef CDynamicPtrArray<CDXLColDescr, CleanupRelease> DrgPdxlcd;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLColDescr
	//
	//	@doc:
	//		Class for representing column descriptors in DXL operators
	//
	//---------------------------------------------------------------------------
	class CDXLColDescr : public CRefCount
	{
		private:
			// memory pool
			IMemoryPool *m_pmp;
			
			// name
			CMDName *m_pmdname;
	
			// column id: unique identifier of that instance of the column in the query
			ULONG m_ulId;
			
			// attribute number in the database (corresponds to varattno in GPDB)
			INT m_iAttno;
			
			// mdid of column's type
			IMDId *m_pmdidType;

			INT m_iTypeModifier;

			// is column dropped from the table: needed for correct restoring of attribute numbers in the range table entries
			BOOL m_fDropped;

			// width of the column, for instance  char(10) column has width 10
			ULONG m_ulWidth;
			
			// private copy ctor
			CDXLColDescr(const CDXLColDescr &);
		
		public:
			// ctor
			CDXLColDescr
				(
				IMemoryPool *,
				CMDName *,
				ULONG ulId,
				INT iAttno,
				IMDId *pmdidType,
				INT iTypeModifier,
				BOOL fDropped,
				ULONG ulWidth = gpos::ulong_max
				);

			//dtor
			~CDXLColDescr();
			
			// column name
			const CMDName *Pmdname() const;

			// column identifier
			ULONG UlID() const;

			// attribute number of the column in the base table
			INT IAttno() const;

			// is the column dropped in the base table
			BOOL FDropped() const;

			// column type
			IMDId *PmdidType() const;

			INT ITypeModifier() const;

			// column width
			ULONG UlWidth() const;
			
			void SerializeToDXL(CXMLSerializer *pxmlser) const;
	};

}



#endif // !GPDXL_CDXLColDescr_H

// EOF
