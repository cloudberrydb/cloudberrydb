//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLWindowSpec.h
//
//	@doc:
//		Class for representing DXL window specification in the DXL
//		representation of the logical query tree
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLWindowSpec_H
#define GPDXL_CDXLWindowSpec_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLWindowFrame.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/md/CMDName.h"

namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLWindowSpec
	//
	//	@doc:
	//		Class for representing DXL window specification in the DXL
	//		representation of the logical query tree
	//
	//---------------------------------------------------------------------------
	class CDXLWindowSpec : public CRefCount
	{
		private:

			// memory pool;
			IMemoryPool *m_pmp;

			// partition-by column identifiers
			DrgPul *m_pdrgpulPartCol;

			// name of window specification
			CMDName *m_pmdname;

			// sorting columns
			CDXLNode *m_pdxlnSortColList;

			// window frame associated with the window key
			CDXLWindowFrame *m_pdxlwf;

			// private copy ctor
			CDXLWindowSpec(const CDXLWindowSpec&);

		public:

			// ctor
			CDXLWindowSpec
				(
				IMemoryPool *pmp,
				DrgPul *pdrgpulPartCol,
				CMDName *pmdname,
				CDXLNode *pdxlnSortColList,
				CDXLWindowFrame *pdxlwf
				);

			// dtor
			virtual
			~CDXLWindowSpec();

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

			// partition-by column identifiers
			DrgPul *PdrgulPartColList() const
			{
				return m_pdrgpulPartCol;
			}

			// sort columns
			CDXLNode *PdxlnSortColList() const
			{
				return m_pdxlnSortColList;
			}

			// window specification name
			CMDName *Pmdname() const
			{
				return m_pmdname;
			}
	};

	typedef CDynamicPtrArray<CDXLWindowSpec, CleanupRelease> DrgPdxlws;
}
#endif // !GPDXL_CDXLWindowSpec_H

// EOF
