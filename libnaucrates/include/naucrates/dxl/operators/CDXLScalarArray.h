//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLScalarArray.h
//
//	@doc:
//		Class for representing DXL scalar arrays
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarArray_H
#define GPDXL_CDXLScalarArray_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"
#include "naucrates/md/IMDId.h"

namespace gpdxl
{
	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarArray
	//
	//	@doc:
	//		Class for representing DXL arrays
	//
	//---------------------------------------------------------------------------
	class CDXLScalarArray : public CDXLScalar
	{
		private:

			// base element type id
			IMDId *m_pmdidElem;
			
			// array type id
			IMDId *m_pmdidArray;

			// is it a multidimensional array
			BOOL m_fMultiDimensional;

			// private copy ctor
			CDXLScalarArray(const CDXLScalarArray&);

		public:
			// ctor
			CDXLScalarArray
				(
				IMemoryPool *pmp,
				IMDId *pmdidElem,
				IMDId *pmdidArray,
				BOOL fMultiDimensional
				);

			// dtor
			virtual
			~CDXLScalarArray();

			// ident accessors
			virtual
			Edxlopid Edxlop() const;

			// operator name
			virtual
			const CWStringConst *PstrOpName() const;

			// element type id
			IMDId *PmdidElem() const;

			// array type id
			IMDId *PmdidArray() const;

			// is array multi-dimensional 
			BOOL FMultiDimensional() const;

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLScalarArray *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarArray == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarArray*>(pdxlop);
			}

			// does the operator return a boolean result
			virtual
			BOOL FBoolean
					(
					CMDAccessor *//pmda
					)
					const
			{
				return false;
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *pdxln, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG
	};
}

#endif // !GPDXL_CDXLScalarArray_H

// EOF
