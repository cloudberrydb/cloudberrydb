//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLTableDescr.h
//
//	@doc:
//		Class for representing table descriptors.
//---------------------------------------------------------------------------



#ifndef GPDXL_CDXLTableDescriptor_H
#define GPDXL_CDXLTableDescriptor_H

#include "gpos/base.h"
#include "naucrates/md/CMDName.h"
#include "naucrates/md/IMDId.h"
#include "naucrates/dxl/operators/CDXLColDescr.h"

namespace gpdxl
{
	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLTableDescr
	//
	//	@doc:
	//		Class for representing table descriptors in a DXL tablescan node.
	//
	//---------------------------------------------------------------------------
	class CDXLTableDescr : public CRefCount
	{
		private:
			// memory pool
			IMemoryPool *m_pmp;
						
			// id and version information for the table
			IMDId *m_pmdid;
			
			// table name
			CMDName *m_pmdname;
	
			// list of column descriptors		
			DrgPdxlcd *m_pdrgdxlcd;
			
			// id of user the table needs to be accessed with
			ULONG m_ulExecuteAsUser;
			
			// private copy ctor
			CDXLTableDescr(const CDXLTableDescr &);
			
			void SerializeMDId(CXMLSerializer *pxmlser) const;
		
		public:
			// ctor/dtor
			CDXLTableDescr(IMemoryPool *pmp, IMDId *pmdid, CMDName *pmdname, ULONG ulExecuteAsUser);
						
			virtual
			~CDXLTableDescr();
		
			// setters
			void SetColumnDescriptors(DrgPdxlcd *pdrgpdxlcd);
			
			void AddColumnDescr(CDXLColDescr *pdxlcd);
			
			// table name
			const CMDName *Pmdname() const;
			
			// table mdid
			IMDId *Pmdid() const;
			
			// table arity
			ULONG UlArity() const;
			
			// user id
			ULONG UlExecuteAsUser() const;
			
			// get the column descriptor at the given position
			const CDXLColDescr *Pdxlcd(ULONG ul) const;
			
			// serialize to dxl format
			void SerializeToDXL(CXMLSerializer *pxmlser) const;
	};
}


#endif // !GPDXL_CDXLTableDescriptor_H

// EOF

