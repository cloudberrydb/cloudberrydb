//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal Inc.
//
//	@filename:
//		CDXLScalarCoerceBase.h
//
//	@doc:
//		Base class for representing DXL coerce operators
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarCoerceBase_H
#define GPDXL_CDXLScalarCoerceBase_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"
#include "naucrates/md/IMDId.h"

namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarCoerceBase
	//
	//	@doc:
	//		Base class for representing DXL coerce operators
	//
	//---------------------------------------------------------------------------
	class CDXLScalarCoerceBase : public CDXLScalar
	{

		private:

			// catalog MDId of the result type
			IMDId *m_pmdidResultType;

			// output type modifier
			INT m_iTypeModifier;

			// coercion form
			EdxlCoercionForm m_edxlcf;

			// location of token to be coerced
			INT m_iLoc;

			// private copy ctor
			CDXLScalarCoerceBase(const CDXLScalarCoerceBase&);

		public:
			// ctor/dtor
			CDXLScalarCoerceBase
				(
				IMemoryPool *pmp,
				IMDId *pmdidType,
				INT iTypeModifier,
				EdxlCoercionForm edxlcf,
				INT iLoc
				);

			virtual
			~CDXLScalarCoerceBase();

			// return result type
			IMDId *PmdidResultType() const
			{
				return m_pmdidResultType;
			}

			// return type modifier
			INT ITypeModifier() const
			{
				return m_iTypeModifier;
			}

			// return coercion form
			EdxlCoercionForm Edxlcf() const
			{
				return m_edxlcf;
			}

			// return token location
			INT ILoc() const
			{
				return m_iLoc;
			}

			// does the operator return a boolean result
			virtual
			BOOL FBoolean(CMDAccessor *pmda) const;

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			virtual
			void AssertValid(const CDXLNode *pdxln, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;
	};
}

#endif // !GPDXL_CDXLScalarCoerceBase_H

// EOF
