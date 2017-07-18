//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal Software, Inc.
//
//	@filename:
//		CDXLPhysicalValuesScan.h
//
//	@doc:
//		Class for representing DXL physical Values scan
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLPhysicalValuesScan_H
#define GPDXL_CDXLPhysicalValuesScan_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLPhysical.h"

namespace gpdxl
{
	enum EdxlnVal
	{
		EdxlValIndexProjList = 0,
		EdxlValIndexConstStart,
		EdxlValIndexSentinel
	};

	// class for representing DXL physical Values scan
	class CDXLPhysicalValuesScan : public CDXLPhysical
	{
		private:
			// private copy ctor
			CDXLPhysicalValuesScan(CDXLPhysicalValuesScan&);

		public:
			// ctor
			CDXLPhysicalValuesScan
				(
					IMemoryPool *pmp
				);

			// dtor
			virtual
			~CDXLPhysicalValuesScan();

			// get operator type
			Edxlopid Edxlop() const;

			// get operator name
			const CWStringConst *PstrOpName() const;

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLPhysicalValuesScan *PdxlopConvert(CDXLOperator *pdxlop);

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG

	};
}

#endif // !GPDXL_CDXLPhysicalValuesScan_H

// EOF
