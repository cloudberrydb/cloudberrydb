//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLLogicalUpdate.h
//
//	@doc:
//		Class for representing logical update operator
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLLogicalUpdate_H
#define GPDXL_CDXLLogicalUpdate_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLLogical.h"

namespace gpdxl
{
	using namespace gpmd;

	// fwd decl
	class CDXLTableDescr;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLLogicalUpdate
	//
	//	@doc:
	//		Class for representing logical update operator
	//
	//---------------------------------------------------------------------------
	class CDXLLogicalUpdate : public CDXLLogical
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

			// list of insertion column ids
			DrgPul *m_pdrgpulInsert;
			
			// should update preserve tuple oids
			BOOL m_fPreserveOids;	

			// tuple oid column id
			ULONG m_ulTupleOid;
			
			// private copy ctor
			CDXLLogicalUpdate(const CDXLLogicalUpdate &);

		public:

			// ctor
			CDXLLogicalUpdate
				(
				IMemoryPool *pmp,
				CDXLTableDescr *pdxltabdesc,
				ULONG ulCtid,
				ULONG ulSegmentId,
				DrgPul *pdrgpulDelete,
				DrgPul *pdrgpulInsert,
				BOOL fPreserveOids,
				ULONG ulTupleOid
				);

			// dtor
			virtual
			~CDXLLogicalUpdate();

			// operator type
			Edxlopid Edxlop() const;

			// operator name
			const CWStringConst *PstrOpName() const;

			// target table descriptor
			CDXLTableDescr *Pdxltabdesc() const
			{
				return m_pdxltabdesc;
			}

			// ctid column id
			ULONG UlCtid() const
			{
				return m_ulCtid;
			}

			// segmentid column id
			ULONG UlSegmentId() const
			{
				return m_ulSegmentId;
			}

			// deletion column ids
			DrgPul *PdrgpulDelete() const
			{
				return m_pdrgpulDelete;
			}

			// insertion column ids
			DrgPul *PdrgpulInsert() const
			{
				return m_pdrgpulInsert;
			}
			
			// does update preserve oids
			BOOL FPreserveOids() const
			{
				return m_fPreserveOids;
			}

			// tuple oid column id
			ULONG UlTupleOid() const
			{
				return m_ulTupleOid;
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
			CDXLLogicalUpdate *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopLogicalUpdate == pdxlop->Edxlop());

				return dynamic_cast<CDXLLogicalUpdate*>(pdxlop);
			}
	};
}

#endif // !GPDXL_CDXLLogicalUpdate_H

// EOF
