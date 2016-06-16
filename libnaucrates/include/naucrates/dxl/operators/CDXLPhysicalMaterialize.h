//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLPhysicalMaterialize.h
//
//	@doc:
//		Class for representing DXL physical materialize operators.
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLPhysicalMaterialize_H
#define GPDXL_CDXLPhysicalMaterialize_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLPhysical.h"
#include "naucrates/dxl/operators/CDXLSpoolInfo.h"


namespace gpdxl
{
	// indices of materialize elements in the children array
	enum Edxlmaterialize
	{
		EdxlmatIndexProjList = 0,
		EdxlmatIndexFilter,
		EdxlmatIndexChild,
		EdxlmatIndexSentinel
	};
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLPhysicalMaterialize
	//
	//	@doc:
	//		Class for representing DXL materialize operators
	//
	//---------------------------------------------------------------------------
	class CDXLPhysicalMaterialize : public CDXLPhysical
	{
		private:
			// eager materialization
			BOOL m_fEager;
			
			// spool info
			// id of the spooling operator
			ULONG m_ulSpoolId;

			// type of the underlying spool
			Edxlspooltype m_edxlsptype;
			
			// slice executing the underlying sort or materialize
			INT m_iExecutorSlice;
			
			// number of consumers in case the materialize is a spooling operator
			ULONG m_ulConsumerSlices;

			// private copy ctor
			CDXLPhysicalMaterialize(CDXLPhysicalMaterialize&);

		public:
			// ctor/dtor
			CDXLPhysicalMaterialize
				(
				IMemoryPool *pmp,
				BOOL fEager
				);
			
			CDXLPhysicalMaterialize
				(
				IMemoryPool *pmp,
				BOOL fEager,
				ULONG ulSpoolId,
				INT iExecutorSlice,
				ULONG ulConsumerSlices
				);

			// accessors
			Edxlopid Edxlop() const;
			const CWStringConst *PstrOpName() const;
			ULONG UlSpoolId() const;
			INT IExecutorSlice() const;
			ULONG UlConsumerSlices() const;
			
			// is the operator spooling to other operators
			BOOL FSpooling() const;

			// does the operator do eager materialization
			BOOL FEager() const;
			
			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLPhysicalMaterialize *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopPhysicalMaterialize == pdxlop->Edxlop());

				return dynamic_cast<CDXLPhysicalMaterialize*>(pdxlop);
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG

	};
}
#endif // !GPDXL_CDXLPhysicalMaterialize_H

// EOF

