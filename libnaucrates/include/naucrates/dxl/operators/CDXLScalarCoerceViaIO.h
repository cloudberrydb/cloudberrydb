//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CDXLScalarCoerceViaIO.h
//
//	@doc:
//		Class for representing DXL CoerceViaIO operation,
//		the operator captures coercing a value from one type to another, by
//		calling the output function of the argument type, and passing the
//		result to the input function of the result type.
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarCoerceViaIO_H
#define GPDXL_CDXLScalarCoerceViaIO_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"
#include "naucrates/md/IMDId.h"

namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarCoerceViaIO
	//
	//	@doc:
	//		Class for representing DXL casting operator
	//
	//---------------------------------------------------------------------------
	class CDXLScalarCoerceViaIO : public CDXLScalar
	{

		private:

			// catalog MDId of the result type
			IMDId *m_pmdidResultType;

			// output type modifications
			INT m_iMod;

			// coercion form
			EdxlCoercionForm m_edxlcf;

			// location of token to be coerced
			INT m_iLoc;

			// private copy ctor
			CDXLScalarCoerceViaIO(const CDXLScalarCoerceViaIO&);

		public:
			// ctor/dtor
			CDXLScalarCoerceViaIO
				(
				IMemoryPool *pmp,
				IMDId *pmdidType,
				INT iMod,
				EdxlCoercionForm edxlcf,
				INT iLoc
				);

			virtual
			~CDXLScalarCoerceViaIO();

			// ident accessor
			virtual
			Edxlopid Edxlop() const
			{
				return EdxlopScalarCoerceViaIO;
			}

			// return result type
			IMDId *PmdidResultType() const
			{
				return m_pmdidResultType;
			}

			// return type modification
			INT IMod() const
			{
				return m_iMod;
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


			// name of the DXL operator name
			virtual
			const CWStringConst *PstrOpName() const;

			// conversion function
			static
			CDXLScalarCoerceViaIO *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarCoerceViaIO == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarCoerceViaIO*>(pdxlop);
			}

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

#endif // !GPDXL_CDXLScalarCoerceViaIO_H

// EOF
