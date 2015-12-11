//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CDXLScalarCoerceToDomain.h
//
//	@doc:
//		Class for representing DXL CoerceToDomain operation,
//		the operator captures coercing a value to a domain type,
//
//		at runtime, the precise set of constraints to be checked against
//		value are determined,
//		if the value passes, it is returned as the result, otherwise an error
//		is raised.
//
//	@owner:
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarCoerceToDomain_H
#define GPDXL_CDXLScalarCoerceToDomain_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"
#include "naucrates/md/IMDId.h"

namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;

	// coercion form
	enum EdxlCoercionForm
	{
		EdxlcfExplicitCall,		// display as a function call
		EdxlcfExplicitCast,		// display as an explicit cast
		EdxlcfImplicitCast,		// implicit cast, so hide it
		EdxlcfDontCare			// don't care about display
	};

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarCoerceToDomain
	//
	//	@doc:
	//		Class for representing DXL casting operator
	//
	//---------------------------------------------------------------------------
	class CDXLScalarCoerceToDomain : public CDXLScalar
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
			CDXLScalarCoerceToDomain(const CDXLScalarCoerceToDomain&);

		public:
			// ctor/dtor
			CDXLScalarCoerceToDomain
				(
				IMemoryPool *pmp,
				IMDId *pmdidType,
				INT iMod,
				EdxlCoercionForm edxlcf,
				INT iLoc
				);

			virtual
			~CDXLScalarCoerceToDomain();

			// ident accessor
			virtual
			Edxlopid Edxlop() const
			{
				return EdxlopScalarCoerceToDomain;
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
			CDXLScalarCoerceToDomain *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarCoerceToDomain == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarCoerceToDomain*>(pdxlop);
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

#endif // !GPDXL_CDXLScalarCoerceToDomain_H

// EOF
