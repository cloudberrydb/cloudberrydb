//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLPhysicalAssert.h
//
//	@doc:
//		Class for representing DXL physical assert operators
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLPhysicalAssert_H
#define GPDXL_CDXLPhysicalAssert_H

#include "gpos/base.h"
#include "naucrates/dxl/errorcodes.h"
#include "naucrates/dxl/operators/CDXLPhysical.h"

namespace gpdxl
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLPhysicalAssert
	//
	//	@doc:
	//		Class for representing DXL assert operators
	//
	//---------------------------------------------------------------------------
	class CDXLPhysicalAssert : public CDXLPhysical
	{
		public:
			// indices of assert elements in the children array
			enum Edxlassert
			{
				EdxlassertIndexProjList = 0,
				EdxlassertIndexFilter,
				EdxlassertIndexChild,
				EdxlassertIndexSentinel
			};
			
		private:
			
			// error code
			CHAR m_szSQLState[GPOS_SQLSTATE_LENGTH + 1];
			
			// private copy ctor
			CDXLPhysicalAssert(CDXLPhysicalAssert&);

		public:
			// ctor
			CDXLPhysicalAssert(IMemoryPool *pmp, const CHAR *szSQLState);

			// dtor
			virtual
			~CDXLPhysicalAssert();
			
			// operator type
			virtual
			Edxlopid Edxlop() const;
			
			// operator name
			virtual
			const CWStringConst *PstrOpName() const;

			// error code
			const CHAR *SzSQLState() const
			{
				return m_szSQLState;
			}
			
			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *pdxln, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG

			// conversion function
			static
			CDXLPhysicalAssert *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopPhysicalAssert == pdxlop->Edxlop());

				return dynamic_cast<CDXLPhysicalAssert*>(pdxlop);
			}
	};
}
#endif // !GPDXL_CDXLPhysicalAssert_H

// EOF

