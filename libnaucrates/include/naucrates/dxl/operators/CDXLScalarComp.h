//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarComp.h
//
//	@doc:
//		Class for representing DXL scalar comparison operators.
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarComp_H
#define GPDXL_CDXLScalarComp_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"
#include "naucrates/md/IMDId.h"

namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;

	// indices of scalar comparison elements in the children array
	enum Edxlsccmp
	{
		EdxlsccmpIndexLeft = 0,
		EdxlsccmpIndexRight,
		EdxlsccmpSentinel
	};

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarComp
	//
	//	@doc:
	//		Class for representing DXL scalar comparison operators
	//
	//---------------------------------------------------------------------------
	class CDXLScalarComp : public CDXLScalar
	{
		
		protected:
						
			// operator number in the catalog
			IMDId *m_pmdid;
					
			// comparison operator name
			const CWStringConst *m_pstrCompOpName;

		private:

			// private copy ctor
			CDXLScalarComp(CDXLScalarComp&);
			
		public:
			// ctor/dtor		
			CDXLScalarComp
				(
				IMemoryPool *pmp,
				IMDId *pmdidOp,
				const CWStringConst *pstrCompOpName
				);
			
			virtual
			~CDXLScalarComp();
			
			// accessor

			// ident accessors
			Edxlopid Edxlop() const;
			
			// name of the DXL operator
			const CWStringConst *PstrOpName() const;

			// name of the comparison operator
			const CWStringConst *PstrCmpOpName() const;
			
			// operator id
			IMDId *Pmdid() const;
						
			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLScalarComp *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarCmp == pdxlop->Edxlop()
						|| EdxlopScalarDistinct == pdxlop->Edxlop()
						|| EdxlopScalarArrayComp == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarComp*>(pdxlop);
			}

			// does the operator return a boolean result
			virtual
			BOOL FBoolean
					(
					CMDAccessor *//pmda
					)
					const

			{
				return true;
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *pdxln, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG
	};
}

#endif // !GPDXL_CDXLScalarComp_H


// EOF
