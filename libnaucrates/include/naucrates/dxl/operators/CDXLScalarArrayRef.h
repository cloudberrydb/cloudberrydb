//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CDXLScalarArrayRef.h
//
//	@doc:
//		Class for representing DXL scalar arrayrefs
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarArrayRef_H
#define GPDXL_CDXLScalarArrayRef_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"
#include "naucrates/md/IMDId.h"

namespace gpdxl
{
	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarArrayRef
	//
	//	@doc:
	//		Class for representing DXL scalar arrayrefs
	//
	//---------------------------------------------------------------------------
	class CDXLScalarArrayRef : public CDXLScalar
	{
		private:

			// base element type id
			IMDId *m_pmdidElem;

			// element type modifier
			INT m_iTypeModifier;

			// array type id
			IMDId *m_pmdidArray;

			// return type id
			IMDId *m_pmdidReturn;

			// private copy ctor
			CDXLScalarArrayRef(const CDXLScalarArrayRef&);

		public:
			// ctor
			CDXLScalarArrayRef
				(
				IMemoryPool *pmp,
				IMDId *pmdidElem,
				INT iTypeModifier,
				IMDId *pmdidArray,
				IMDId *pmdidReturn
				);

			// dtor
			virtual
			~CDXLScalarArrayRef();

			// ident accessors
			virtual
			Edxlopid Edxlop() const;

			// operator name
			virtual
			const CWStringConst *PstrOpName() const;

			// element type id
			IMDId *PmdidElem() const
			{
				return m_pmdidElem;
			}

			// element type modifier
			INT
			ITypeModifier() const;

			// array type id
			IMDId *PmdidArray() const
			{
				return m_pmdidArray;
			}

			// return type id
			IMDId *PmdidReturn() const
			{
				return m_pmdidReturn;
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
			virtual
			void AssertValid(const CDXLNode *pdxln, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG

			// conversion function
			static
			CDXLScalarArrayRef *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarArrayRef == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarArrayRef*>(pdxlop);
			}
	};
}

#endif // !GPDXL_CDXLScalarArrayRef_H

// EOF
