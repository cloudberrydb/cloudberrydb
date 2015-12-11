//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLIndexDescr.h
//
//	@doc:
//		Class for representing index descriptors
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLIndexDescriptor_H
#define GPDXL_CDXLIndexDescriptor_H

#include "gpos/base.h"
#include "naucrates/md/CMDName.h"
#include "naucrates/md/IMDId.h"

namespace gpdxl
{
	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLIndexDescr
	//
	//	@doc:
	//		Class for representing index descriptors in a DXL index scan node.
	//
	//---------------------------------------------------------------------------
	class CDXLIndexDescr : public CRefCount
	{
		private:
			// memory pool
			IMemoryPool *m_pmp;

			// id and version information for the table
			IMDId *m_pmdid;

			// index name
			CMDName *m_pmdname;

			// private copy ctor
			CDXLIndexDescr(const CDXLIndexDescr &);

		public:
			// ctor
			CDXLIndexDescr(IMemoryPool *pmp, IMDId *pmdid, CMDName *pmdname);

			// dtor
			virtual
			~CDXLIndexDescr();

			// accessors
			const CMDName *Pmdname() const;
			IMDId *Pmdid() const;

			// serialize the operator to a DXL document
			void SerializeToDXL(CXMLSerializer *) const;
	};
}

#endif // !GPDXL_CDXLIndexDescriptor_H

// EOF

