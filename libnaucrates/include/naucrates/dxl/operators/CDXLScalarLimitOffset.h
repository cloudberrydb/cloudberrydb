//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarLimitOffset.h
//
//	@doc:
//		Class for representing DXL scalar LimitOffset (This is only used by CDXLPhysicalLimit)
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarLimitOffset_H
#define GPDXL_CDXLScalarLimitOffset_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"


namespace gpdxl
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarLimitOffset
	//
	//	@doc:
	//		Class for representing DXL scalar LimitOffset
	//
	//---------------------------------------------------------------------------
	class CDXLScalarLimitOffset : public CDXLScalar
	{
		private:
				// private copy ctor
			CDXLScalarLimitOffset(const CDXLScalarLimitOffset&);

		public:

			// ctor/dtor
			explicit
			CDXLScalarLimitOffset(IMemoryPool *pmp);

			// ident accessors
			Edxlopid Edxlop() const;

			// name of the DXL operator
			const CWStringConst *PstrOpName() const;

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *, const CDXLNode *) const;

			// conversion function
			static
			CDXLScalarLimitOffset *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarLimitOffset == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarLimitOffset*>(pdxlop);
			}

			// does the operator return a boolean result
			virtual
			BOOL FBoolean
					(
					CMDAccessor *//pmda
					)
					const
			{
				GPOS_ASSERT(!"Invalid function call for a container operator");
				return false;
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *pdxln, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG
	};
}
#endif // !GPDXL_CDXLScalarLimitOffset_H

// EOF
