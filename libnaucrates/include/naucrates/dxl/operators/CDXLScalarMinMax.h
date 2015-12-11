//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 Pivotal Inc.
//
//	@filename:
//		CDXLScalarMinMax.h
//
//	@doc:
//		Class for representing DXL MinMax operator
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarMinMax_H
#define GPDXL_CDXLScalarMinMax_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"
#include "naucrates/md/IMDId.h"

namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarMinMax
	//
	//	@doc:
	//		Class for representing DXL MinMax operator
	//
	//---------------------------------------------------------------------------
	class CDXLScalarMinMax : public CDXLScalar
	{
		public:

			// types of operations: either min or max
			enum EdxlMinMaxType
				{
					EmmtMin,
					EmmtMax,
					EmmtSentinel
				};

		private:
			// return type
			IMDId *m_pmdidType;

			// min/max type
			EdxlMinMaxType m_emmt;

			// private copy ctor
			CDXLScalarMinMax(const CDXLScalarMinMax&);

		public:

			// ctor
			CDXLScalarMinMax(IMemoryPool *pmp, IMDId *pmdidType, EdxlMinMaxType emmt);

			//dtor
			virtual
			~CDXLScalarMinMax();

			// name of the operator
			virtual
			const CWStringConst *PstrOpName() const;

			// return type
			virtual
			IMDId *PmdidType() const
			{
				return m_pmdidType;
			}

			// DXL Operator ID
			virtual
			Edxlopid Edxlop() const;

			// min/max type
			EdxlMinMaxType Emmt() const
			{
				return m_emmt;
			}

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// does the operator return a boolean result
			virtual
			BOOL FBoolean(CMDAccessor *pmda) const;

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *pdxln, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG

			// conversion function
			static
			CDXLScalarMinMax *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarMinMax == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarMinMax*>(pdxlop);
			}
	};
}
#endif // !GPDXL_CDXLScalarMinMax_H

// EOF
