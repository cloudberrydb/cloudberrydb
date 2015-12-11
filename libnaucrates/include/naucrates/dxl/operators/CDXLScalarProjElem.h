//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarProjElem.h
//
//	@doc:
//		Class for representing DXL projection lists.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------



#ifndef GPDXL_CDXLScalarProjElem_H
#define GPDXL_CDXLScalarProjElem_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"
#include "naucrates/md/CMDName.h"

namespace gpdxl
{
	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarProjElem
	//
	//	@doc:
	//		Container for projection list elements, storing the expression and the alias
	//
	//---------------------------------------------------------------------------
	class CDXLScalarProjElem : public CDXLScalar
	{
		private:
			// id of column defined by this project element:
			// for computed columns this is a new id, for colrefs: id of the original column
			ULONG m_ulId;

			// alias
			const CMDName *m_pmdname;
				
			// private copy ctor
			CDXLScalarProjElem(CDXLScalarProjElem&);
			
		public:
			// ctor/dtor
			CDXLScalarProjElem
				(
				IMemoryPool *pmp,
				ULONG ulId,
				const CMDName *pmdname
				);
			
			virtual
			~CDXLScalarProjElem();
			
			// ident accessors
			Edxlopid Edxlop() const;
			
			// name of the operator
			const CWStringConst *PstrOpName() const;
			
			// id of the proj element
			ULONG UlId() const;
			
			// alias of the proj elem
			const CMDName *PmdnameAlias() const;
			
			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *, const CDXLNode *) const;

			// check if given column is defined by operator
			virtual
			BOOL FDefinesColumn
				(
				ULONG ulColId
				)
				const
			{
				return (UlId() == ulColId);
			}

			// conversion function
			static
			CDXLScalarProjElem *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarProjectElem == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarProjElem*>(pdxlop);
			}

			// does the operator return a boolean result
			virtual
			BOOL FBoolean
					(
					CMDAccessor *//pmda
					)
					const
			{
				GPOS_ASSERT(!"Invalid function call on a container operator");
				return false;
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure
			void AssertValid(const CDXLNode *pdxln, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG
	};
}

#endif // !GPDXL_CDXLScalarProjElem_H

// EOF

