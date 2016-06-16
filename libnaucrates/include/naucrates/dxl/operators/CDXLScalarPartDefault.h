//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CDXLScalarPartDefault.h
//
//	@doc:
//		Class for representing DXL Part Default expressions
//		These expressions indicate whether a particular part is a default part
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarPartDefault_H
#define GPDXL_CDXLScalarPartDefault_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"

namespace gpdxl
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarPartDefault
	//
	//	@doc:
	//		Class for representing DXL Part Default expressions
	//
	//---------------------------------------------------------------------------
	class CDXLScalarPartDefault : public CDXLScalar
	{
		private:

			// partitioning level
			ULONG m_ulLevel;

			// private copy ctor
			CDXLScalarPartDefault(const CDXLScalarPartDefault&);

		public:
			// ctor
			CDXLScalarPartDefault(IMemoryPool *pmp, ULONG ulLevel);

			// operator type
			virtual
			Edxlopid Edxlop() const;

			// operator name
			virtual
			const CWStringConst *PstrOpName() const;

			// partitioning level
			ULONG UlLevel() const
			{
				return m_ulLevel;
			}

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// does the operator return a boolean result
			virtual
			BOOL FBoolean
					(
					CMDAccessor * //pmda
					)
					const
			{
				return true;
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			virtual
			void AssertValid(const CDXLNode *pdxln, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG

			// conversion function
			static
			CDXLScalarPartDefault *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarPartDefault == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarPartDefault*>(pdxlop);
			}
	};
}

#endif // !GPDXL_CDXLScalarPartDefault_H

// EOF
