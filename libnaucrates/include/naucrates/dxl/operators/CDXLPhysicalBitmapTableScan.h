//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CDXLPhysicalBitmapTableScan.h
//
//	@doc:
//		Class for representing DXL bitmap table scan operators.
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLPhysicalBitmapTableScan_H
#define GPDXL_CDXLPhysicalBitmapTableScan_H

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
	//		CDXLPhysicalBitmapTableScan
	//
	//	@doc:
	//		Class for representing DXL bitmap table scan operators
	//
	//---------------------------------------------------------------------------
	class CDXLPhysicalBitmapTableScan : public CDXLPhysicalAbstractBitmapScan
	{
		private:
			// private copy ctor
			CDXLPhysicalBitmapTableScan(const CDXLPhysicalBitmapTableScan &);

		public:
			// ctors
			CDXLPhysicalBitmapTableScan
				(
				IMemoryPool *pmp,
				CDXLTableDescr *pdxltabdesc
				)
				:
				CDXLPhysicalAbstractBitmapScan(pmp, pdxltabdesc)
			{
			}

			// dtor
			virtual
			~CDXLPhysicalBitmapTableScan()
			{}

			// operator type
			virtual
			Edxlopid Edxlop() const
			{
				return EdxlopPhysicalBitmapTableScan;
			}

			// operator name
			virtual
			const CWStringConst *PstrOpName() const;

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLPhysicalBitmapTableScan *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopPhysicalBitmapTableScan == pdxlop->Edxlop());

 	 	 		return dynamic_cast<CDXLPhysicalBitmapTableScan *>(pdxlop);
			}

	};  // class CDXLPhysicalBitmapTableScan
}

#endif  // !GPDXL_CDXLPhysicalBitmapTableScan_H

// EOF
