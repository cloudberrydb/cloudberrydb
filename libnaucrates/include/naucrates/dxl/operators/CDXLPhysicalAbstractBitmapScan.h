//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CDXLPhysicalAbstractBitmapScan.h
//
//	@doc:
//		Parent class for representing DXL bitmap table scan operators,
//		both not partitioned and dynamic.
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLPhysicalAbstractBitmapScan_H
#define GPDXL_CDXLPhysicalAbstractBitmapScan_H

#include "gpos/base.h"

#include "naucrates/dxl/operators/CDXLPhysical.h"

namespace gpdxl
{
	using namespace gpos;

	// fwd declarations
	class CDXLTableDescr;
	class CXMLSerializer;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLPhysicalAbstractBitmapScan
	//
	//	@doc:
	//		Parent class for representing DXL bitmap table scan operators, both not
	//		partitioned and dynamic.
	//
	//---------------------------------------------------------------------------
	class CDXLPhysicalAbstractBitmapScan : public CDXLPhysical
	{
		private:
			// private copy ctor
			CDXLPhysicalAbstractBitmapScan(const CDXLPhysicalAbstractBitmapScan &);

		protected:
			// table descriptor for the scanned table
			CDXLTableDescr *m_pdxltabdesc;

		public:
			// ctor
			CDXLPhysicalAbstractBitmapScan
				(
				IMemoryPool *pmp,
				CDXLTableDescr *pdxltabdesc
				)
				:
				CDXLPhysical(pmp),
				m_pdxltabdesc(pdxltabdesc)
			{
				GPOS_ASSERT(NULL != pdxltabdesc);
			}

			// dtor
			virtual
			~CDXLPhysicalAbstractBitmapScan();

			// table descriptor
			const CDXLTableDescr *Pdxltabdesc()
			{
				return m_pdxltabdesc;
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			virtual
			void AssertValid(const CDXLNode *pdxln, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG
	};  // class CDXLPhysicalAbstractBitmapScan
}

#endif  // !GPDXL_CDXLPhysicalAbstractBitmapScan_H

// EOF
