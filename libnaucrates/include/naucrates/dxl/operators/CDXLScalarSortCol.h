//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarSortCol.h
//
//	@doc:
//		Class for representing sorting column info in DXL Sort and Motion nodes.
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarSortCol_H
#define GPDXL_CDXLScalarSortCol_H


#include "gpos/base.h"

#include "naucrates/dxl/gpdb_types.h"
#include "naucrates/dxl/operators/CDXLScalar.h"
#include "naucrates/md/IMDId.h"

namespace gpdxl
{

	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarSortCol
	//
	//	@doc:
	//		Sorting column info in DXL Sort and Motion nodes
	//
	//---------------------------------------------------------------------------
	class CDXLScalarSortCol : public CDXLScalar
	{
		private:
		
			// id of the sorting column
			ULONG m_ulColId;
			
			// catalog Oid of the sorting operator
			IMDId *m_pmdidSortOp;
			
			// name of sorting operator 
			CWStringConst *m_pstrSortOpName;
			
			// sort nulls before other values
			BOOL m_fSortNullsFirst;
			
			// private copy ctor
			CDXLScalarSortCol(CDXLScalarSortCol&);
			
		public:
			// ctor/dtor
			CDXLScalarSortCol
				(
				IMemoryPool *pmp,
				ULONG ulColId,
				IMDId *pmdidSortOp,
				CWStringConst *pstrTypeName,
				BOOL fSortNullsFirst
				);
			
			virtual
			~CDXLScalarSortCol();

			// ident accessors
			Edxlopid Edxlop() const;
			
			// name of the operator
			const CWStringConst *PstrOpName() const;
			
			// Id of the sorting column
			ULONG UlColId() const;
			
			// mdid of the sorting operator
			IMDId *PmdidSortOp() const;
			
			// whether nulls are sorted before other values
			BOOL FSortNullsFirst() const;
			
			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *, const CDXLNode *) const;

			// conversion function
			static
			CDXLScalarSortCol *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarSortCol == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarSortCol*>(pdxlop);
			}

			// does the operator return a boolean result
			virtual
			BOOL FBoolean
					(
					CMDAccessor *//pmda
					)
					const
			{
				GPOS_ASSERT(!"Invalid function call for this operator");
				return false;
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *pdxln, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG			
	};
}

#endif // !GPDXL_CDXLScalarSortCol_H

// EOF
