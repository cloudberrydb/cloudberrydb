//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC, Corp.
//
//	@filename:
//		CDXLScalarSubquery.h
//
//	@doc:
//		Class for representing subqueries computing scalar values
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarSubquery_H
#define GPDXL_CDXLScalarSubquery_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/operators/CDXLScalar.h"

namespace gpdxl
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarSubquery
	//
	//	@doc:
	//		Class for representing subqueries computing scalar values
	//
	//---------------------------------------------------------------------------
	class CDXLScalarSubquery : public CDXLScalar
	{
		private:
			// id of column computed by the subquery
			ULONG m_ulColId;
			
			// private copy ctor
			CDXLScalarSubquery(CDXLScalarSubquery&);
			
		public:
			// ctor/dtor
			CDXLScalarSubquery(IMemoryPool *pmp, ULONG ulColId);
			
			virtual
			~CDXLScalarSubquery();

			// ident accessors
			Edxlopid Edxlop() const;
			
			// colid of subquery column
			ULONG UlColId() const
			{
				return m_ulColId;
			}
			
			// name of the operator
			const CWStringConst *PstrOpName() const;
			
			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *, const CDXLNode *) const;

			// conversion function
			static
			CDXLScalarSubquery *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarSubquery == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarSubquery*>(pdxlop);
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

#endif // !GPDXL_CDXLScalarSubquery_H

// EOF
