//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLPhysicalSubqueryScan.h
//
//	@doc:
//		Class for representing DXL physical subquery scan (projection) operators.
//---------------------------------------------------------------------------



#ifndef GPDXL_CDXLPhysicalSubqueryScan_H
#define GPDXL_CDXLPhysicalSubqueryScan_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLPhysical.h"
#include "naucrates/md/CMDName.h"


namespace gpdxl
{
	using namespace gpmd;

	// indices of subquery scan elements in the children array
	enum Edxlsubqscan
	{
		EdxlsubqscanIndexProjList = 0,
		EdxlsubqscanIndexFilter,
		EdxlsubqscanIndexChild,
		EdxlsubqscanIndexSentinel
	};
	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLPhysicalSubqueryScan
	//
	//	@doc:
	//		Class for representing DXL physical subquery scan (projection) operators
	//
	//---------------------------------------------------------------------------
	class CDXLPhysicalSubqueryScan : public CDXLPhysical
	{
		private:
		
			// name for the subquery scan node (corresponding to name in GPDB's SubqueryScan)
			CMDName *m_pmdnameAlias;
			
			// private copy ctor
			CDXLPhysicalSubqueryScan(CDXLPhysicalSubqueryScan&);

		public:
			// ctor/dtor
			CDXLPhysicalSubqueryScan(IMemoryPool *pmp, CMDName *pmdname);

			virtual
			~CDXLPhysicalSubqueryScan();
						
			// accessors
			Edxlopid Edxlop() const;
			const CWStringConst *PstrOpName() const;
			const CMDName *Pmdname();
			
			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLPhysicalSubqueryScan *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopPhysicalSubqueryScan == pdxlop->Edxlop());

				return dynamic_cast<CDXLPhysicalSubqueryScan*>(pdxlop);
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG
			
	};
}
#endif // !GPDXL_CDXLPhysicalSubqueryScan_H

// EOF

