//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarDistinctComp.h
//
//	@doc:
//		Class for representing DXL scalar "is distinct from" comparison operators.
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarDistinctComp_H
#define GPDXL_CDXLScalarDistinctComp_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalarComp.h"


namespace gpdxl
{
	// indices of scalar distinct comparison elements in the children array
	enum Edxlscdistcmp
	{
		EdxlscdistcmpIndexLeft = 0,
		EdxlscdistcmpIndexRight,
		EdxlscdistcmpSentinel
	};

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarDistinctComp
	//
	//	@doc:
	//		Class for representing DXL scalar "is distinct from" comparison operators
	//
	//---------------------------------------------------------------------------
	class CDXLScalarDistinctComp : public CDXLScalarComp
	{
		
		private:
						
			// private copy ctor
			CDXLScalarDistinctComp(CDXLScalarDistinctComp&);
			
		public:
			// ctor/dtor		
			CDXLScalarDistinctComp
				(
				IMemoryPool *pmp,
				IMDId *pmdidOp
				);
						
			// ident accessors
			Edxlopid Edxlop() const;
			
			// name of the DXL operator
			const CWStringConst *PstrOpName() const;
						
			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLScalarDistinctComp *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarDistinct == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarDistinctComp*>(pdxlop);
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

#endif // !GPDXL_CDXLScalarDistinctComp_H


// EOF
