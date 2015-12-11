//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLLogicalSetOp.h
//
//	@doc:
//		Class for representing DXL logical set operators
//		
//	@owner: 
//		
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPDXL_CDXLLogicalSetOp_H
#define GPDXL_CDXLLogicalSetOp_H

#include "gpos/base.h"

#include "naucrates/dxl/operators/CDXLLogical.h"
#include "naucrates/dxl/operators/CDXLColDescr.h"

namespace gpdxl
{

	enum EdxlSetOpType
	{
		EdxlsetopUnion,
		EdxlsetopUnionAll,
		EdxlsetopIntersect,
		EdxlsetopIntersectAll,
		EdxlsetopDifference,
		EdxlsetopDifferenceAll,
		EdxlsetopSentinel
	};

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLLogicalSetOp
	//
	//	@doc:
	//		Class for representing DXL logical set operators
	//
	//---------------------------------------------------------------------------
	class CDXLLogicalSetOp : public CDXLLogical
	{
		private:

			// private copy ctor
			CDXLLogicalSetOp(CDXLLogicalSetOp&);

			// set operation type
			EdxlSetOpType m_edxlsetoptype;

			// list of output column descriptors
			DrgPdxlcd *m_pdrgpdxlcd;

			// array of input colid arrays
			DrgPdrgPul *m_pdrgpdrgpul;
			
			// do the columns need to be casted accross inputs
			BOOL m_fCastAcrossInputs;

		public:
			// ctor
			CDXLLogicalSetOp
				(
				IMemoryPool *pmp,
				EdxlSetOpType edxlsetoptype,
				DrgPdxlcd *pdrgdxlcd,
				DrgPdrgPul *pdrgpdrgpul,
				BOOL fCastAcrossInput
				);

			// dtor
			virtual
			~CDXLLogicalSetOp();

			// operator id
			Edxlopid Edxlop() const;

			// operator name
			const CWStringConst *PstrOpName() const;

			// set operator type
			EdxlSetOpType Edxlsetoptype() const
			{
				return m_edxlsetoptype;
			}

			// array of output columns
			const DrgPdxlcd *Pdrgpdxlcd() const
			{
				return m_pdrgpdxlcd;
			}

			// number of output columns
			ULONG UlArity() const
			{
				return m_pdrgpdxlcd->UlLength();
			}

			// output column descriptor at a given position
			const CDXLColDescr *Pdxlcd
				(
				ULONG ulPos
				)
				const
			{
				return (*m_pdrgpdxlcd)[ulPos];
			}

			// number of inputs to the n-ary set operation
		    ULONG UlChildren() const
			{
				return m_pdrgpdrgpul->UlLength();	
			}
		
			// column array of the input at a given position 
			const DrgPul *Pdrgpul
				(
				ULONG ulPos
				)
				const
			{
				GPOS_ASSERT(ulPos < UlChildren());
				
				return (*m_pdrgpdrgpul)[ulPos];
			}
		
			// do the columns across inputs need to be casted
			BOOL FCastAcrossInputs() const
			{
				return m_fCastAcrossInputs;
			}

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// check if given column is defined by operator
			virtual
			BOOL FDefinesColumn(ULONG ulColId) const;

			// conversion function
			static
			CDXLLogicalSetOp *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopLogicalSetOp == pdxlop->Edxlop());

				return dynamic_cast<CDXLLogicalSetOp*>(pdxlop);
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG

	};
}
#endif // !GPDXL_CDXLLogicalSetOp_H

// EOF
