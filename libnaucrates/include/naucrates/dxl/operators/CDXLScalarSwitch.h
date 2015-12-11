//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLScalarSwitch.h
//
//	@doc:
//
//		Class for representing DXL Switch (corresponds to Case (expr) ...)
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarSwitch_H
#define GPDXL_CDXLScalarSwitch_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"
#include "naucrates/md/IMDId.h"

namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarSwitch
	//
	//	@doc:
	//		Class for representing DXL Switch
	//
	//---------------------------------------------------------------------------
	class CDXLScalarSwitch : public CDXLScalar
	{
		private:
			// return type
			IMDId *m_pmdidType;

			// private copy ctor
			CDXLScalarSwitch(const CDXLScalarSwitch&);

		public:

			// ctor
			CDXLScalarSwitch(IMemoryPool *pmp, IMDId *pmdidType);

			//dtor
			virtual
			~CDXLScalarSwitch();

			// name of the operator
			virtual
			const CWStringConst *PstrOpName() const;

			// return type
			virtual
			IMDId *PmdidType() const;

			// DXL Operator ID
			virtual
			Edxlopid Edxlop() const;

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLScalarSwitch *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarSwitch == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarSwitch*>(pdxlop);
			}

			// does the operator return a boolean result
			virtual
			BOOL FBoolean(CMDAccessor *pmda) const;

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *pdxln, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG
	};
}
#endif // !GPDXL_CDXLScalarSwitch_H

// EOF
