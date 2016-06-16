//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarLimitCount.h
//
//	@doc:
//		Class for representing DXL scalar LimitCount (Limit Count Node is only used by CDXLPhysicalLimit
//		
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarLimitCount_H
#define GPDXL_CDXLScalarLimitCount_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"


namespace gpdxl
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarLimitCount
	//
	//	@doc:
	//		Class for representing DXL scalar LimitCount
	//
	//---------------------------------------------------------------------------
	class CDXLScalarLimitCount : public CDXLScalar
	{
		private:
				// private copy ctor
			CDXLScalarLimitCount(const CDXLScalarLimitCount&);

		public:

			// ctor/dtor
			explicit
			CDXLScalarLimitCount(IMemoryPool *pmp);

			// ident accessors
			Edxlopid Edxlop() const;

			// name of the DXL operator
			const CWStringConst *PstrOpName() const;

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *, const CDXLNode *) const;

			// conversion function
			static
			CDXLScalarLimitCount *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarLimitCount == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarLimitCount*>(pdxlop);
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
#endif // !GPDXL_CDXLScalarLimitCount_H

// EOF
