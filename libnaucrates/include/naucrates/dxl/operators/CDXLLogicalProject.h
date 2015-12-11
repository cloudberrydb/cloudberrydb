//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLLogicalProject.h
//
//	@doc:
//		Class for representing DXL logical project operators
//		
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPDXL_CDXLLogicalProject_H
#define GPDXL_CDXLLogicalProject_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLLogical.h"

namespace gpdxl
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLLogicalProject
	//
	//	@doc:
	//		Class for representing DXL logical project operators
	//
	//---------------------------------------------------------------------------
	class CDXLLogicalProject : public CDXLLogical
	{
		private:

			// private copy ctor
			CDXLLogicalProject(CDXLLogicalProject&);

			// alias name
			const CMDName *m_pmdnameAlias;

		public:
			// ctor
			explicit
			CDXLLogicalProject(IMemoryPool *);

			// accessors
			Edxlopid Edxlop() const;
			const CWStringConst *PstrOpName() const;
			const CMDName *Pmdname() const;

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// set alias name
			void SetAliasName(CMDName *);

			// conversion function
			static
			CDXLLogicalProject *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopLogicalProject == pdxlop->Edxlop());

				return dynamic_cast<CDXLLogicalProject*>(pdxlop);
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG

	};
}
#endif // !GPDXL_CDXLLogicalProject_H

// EOF
