//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CDXLPhysicalDynamicBitmapTableScan.h
//
//	@doc:
//		Class for representing dynamic DXL bitmap table scan operators.
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLPhysicalDynamicBitmapTableScan_H
#define GPDXL_CDXLPhysicalDynamicBitmapTableScan_H

#include "gpos/base.h"

#include "naucrates/dxl/operators/CDXLPhysicalAbstractBitmapScan.h"

namespace gpdxl
{
	using namespace gpos;

	// fwd declarations
	class CDXLTableDescr;
	class CXMLSerializer;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLPhysicalDynamicBitmapTableScan
	//
	//	@doc:
	//		Class for representing DXL bitmap table scan operators
	//
	//---------------------------------------------------------------------------
	class CDXLPhysicalDynamicBitmapTableScan : public CDXLPhysicalAbstractBitmapScan
	{
		private:
			// id of partition index structure
			ULONG m_ulPartIndexId;

			// printable partition index id
			ULONG m_ulPartIndexIdPrintable;

			// private copy ctor
			CDXLPhysicalDynamicBitmapTableScan(const CDXLPhysicalDynamicBitmapTableScan &);

		public:
			// ctor
			CDXLPhysicalDynamicBitmapTableScan
				(
				IMemoryPool *pmp,
				CDXLTableDescr *pdxltabdesc,
				ULONG ulPartIndexId,
				ULONG ulPartIndexIdPrintable
				)
				:
				CDXLPhysicalAbstractBitmapScan(pmp, pdxltabdesc),
				m_ulPartIndexId(ulPartIndexId),
				m_ulPartIndexIdPrintable(ulPartIndexIdPrintable)
			{
				GPOS_ASSERT(NULL != pdxltabdesc);
			}

			// dtor
			virtual
			~CDXLPhysicalDynamicBitmapTableScan()
			{}

			// operator type
			virtual
			Edxlopid Edxlop() const
			{
				return EdxlopPhysicalDynamicBitmapTableScan;
			}

			// operator name
			virtual
			const CWStringConst *PstrOpName() const;

			// partition index id
			ULONG UlPartIndexId() const
			{
				return m_ulPartIndexId;
			}

			// printable partition index id
			ULONG UlPartIndexIdPrintable() const
			{
				return m_ulPartIndexIdPrintable;
			}

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLPhysicalDynamicBitmapTableScan *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopPhysicalDynamicBitmapTableScan == pdxlop->Edxlop());

 	 	 		return dynamic_cast<CDXLPhysicalDynamicBitmapTableScan *>(pdxlop);
			}

	};  // class CDXLPhysicalDynamicBitmapTableScan
}

#endif  // !GPDXL_CDXLPhysicalDynamicBitmapTableScan_H

// EOF
