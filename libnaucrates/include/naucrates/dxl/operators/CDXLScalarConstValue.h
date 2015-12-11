//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarConstValue.h
//
//	@doc:
//		Class for representing DXL Const Value
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarConstValue_H
#define GPDXL_CDXLScalarConstValue_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"
#include "naucrates/dxl/operators/CDXLDatum.h"
#include "naucrates/dxl/gpdb_types.h"

namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarConstValue
	//
	//	@doc:
	//		Class for representing DXL scalar Const value
	//
	//---------------------------------------------------------------------------
	class CDXLScalarConstValue : public CDXLScalar
	{
		private:

			CDXLDatum *m_pdxldatum;

			// private copy ctor
			CDXLScalarConstValue(const CDXLScalarConstValue&);

		public:

			// ctor/dtor
			CDXLScalarConstValue
				(
				IMemoryPool *pmp,
				CDXLDatum *pdxldatum
				);

			virtual
			~CDXLScalarConstValue();

			// name of the operator
			const CWStringConst *PstrOpName() const;

			// return the datum value
			const CDXLDatum* Pdxldatum() const
			{
				return m_pdxldatum;
			}

			// DXL Operator ID
			Edxlopid Edxlop() const;
			
			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLScalarConstValue *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarConstValue == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarConstValue*>(pdxlop);
			}

			// does the operator return a boolean result
			virtual
			BOOL FBoolean(CMDAccessor *pmda) const;

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure
			void AssertValid(const CDXLNode *pdxln, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG
	};
}
#endif // !GPDXL_CDXLScalarConstValue_H

// EOF
