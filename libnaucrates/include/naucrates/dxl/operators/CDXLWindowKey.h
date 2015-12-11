//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLWindowKey.h
//
//	@doc:
//		Class for representing DXL window key
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLWindowKey_H
#define GPDXL_CDXLWindowKey_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLWindowFrame.h"

namespace gpdxl
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLWindowKey
	//
	//	@doc:
	//		Class for representing DXL window key
	//
	//---------------------------------------------------------------------------
	class CDXLWindowKey : public CRefCount
	{
		private:

			// memory pool;
			IMemoryPool *m_pmp;

			// window frame associated with the window key
			CDXLWindowFrame *m_pdxlwf;

			// private copy ctor
			CDXLWindowKey(const CDXLWindowKey&);

			// sorting columns
			CDXLNode *m_pdxlnSortColList;

		public:

			// ctor
			explicit
			CDXLWindowKey(IMemoryPool *pmp);

			// dtor
			virtual
			~CDXLWindowKey();

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *) const;

			// set window frame definition
			void SetWindowFrame(CDXLWindowFrame *pdxlwf);

			// return window frame
			CDXLWindowFrame *Pdxlwf() const
			{
				return m_pdxlwf;
			}

			// set the list of sort columns
			void SetSortColList(CDXLNode *pdxlnSortColList);

			// sort columns
			CDXLNode *PdxlnSortColList() const
			{
				return m_pdxlnSortColList;
			}
	};

	typedef CDynamicPtrArray<CDXLWindowKey, CleanupRelease> DrgPdxlwk;
}
#endif // !GPDXL_CDXLWindowKey_H

// EOF
