//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLLogicalDelete.h
//
//	@doc:
//		Class for representing logical delete operator
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLLogicalDelete_H
#define GPDXL_CDXLLogicalDelete_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLLogical.h"

namespace gpdxl
{
	using namespace gpmd;

	// fwd decl
	class CDXLTableDescr;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLLogicalDelete
	//
	//	@doc:
	//		Class for representing logical delete operator
	//
	//---------------------------------------------------------------------------
	class CDXLLogicalDelete : public CDXLLogical
	{
		private:

			// target table descriptor
			CDXLTableDescr *m_pdxltabdesc;

			// ctid column id
			ULONG m_ulCtid;

			// segmentId column id
			ULONG m_ulSegmentId;

			// list of deletion column ids
			DrgPul *m_pdrgpulDelete;

			// private copy ctor
			CDXLLogicalDelete(const CDXLLogicalDelete &);

		public:

			// ctor
			CDXLLogicalDelete(IMemoryPool *pmp, CDXLTableDescr *pdxltabdesc, ULONG ulCtid, ULONG ulSegmentId, DrgPul *pdrgpulDelete);

			// dtor
			virtual
			~CDXLLogicalDelete();

			// operator type
			Edxlopid Edxlop() const;

			// operator name
			const CWStringConst *PstrOpName() const;

			// target table descriptor
			CDXLTableDescr *Pdxltabdesc() const
			{
				return m_pdxltabdesc;
			}

			// ctid column
			ULONG UlCtid() const
			{
				return m_ulCtid;
			}

			// segment id column
			ULONG UlSegmentId() const
			{
				return m_ulSegmentId;
			}

			// deletion column ids
			DrgPul *PdrgpulDelete() const
			{
				return m_pdrgpulDelete;
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *pdxln, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLLogicalDelete *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopLogicalDelete == pdxlop->Edxlop());

				return dynamic_cast<CDXLLogicalDelete*>(pdxlop);
			}

	};
}

#endif // !GPDXL_CDXLLogicalDelete_H

// EOF
