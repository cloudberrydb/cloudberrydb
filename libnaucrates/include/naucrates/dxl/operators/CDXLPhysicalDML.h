//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLPhysicalDML.h
//
//	@doc:
//		Class for representing physical DML operator
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLPhysicalDML_H
#define GPDXL_CDXLPhysicalDML_H

#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"

#include "naucrates/dxl/operators/CDXLPhysical.h"

namespace gpdxl
{
	// fwd decl
	class CDXLTableDescr;
	class CDXLDirectDispatchInfo;
	
	enum EdxlDmlType
		{
			Edxldmlinsert,
			Edxldmldelete,
			Edxldmlupdate,
			EdxldmlSentinel
		};

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLPhysicalDML
	//
	//	@doc:
	//		Class for representing physical DML operators
	//
	//---------------------------------------------------------------------------
	class CDXLPhysicalDML : public CDXLPhysical
	{
		private:

			// operator type
			const EdxlDmlType m_edxldmltype;

			// target table descriptor
			CDXLTableDescr *m_pdxltabdesc;

			// list of source column ids		
			DrgPul *m_pdrgpul;
			
			// action column id
			ULONG m_ulAction;

			// oid column id
			ULONG m_ulOid;

			// ctid column id
			ULONG m_ulCtid;

			// segmentid column id
			ULONG m_ulSegmentId;

			// should update preserve tuple oids
			BOOL m_fPreserveOids;	

			// tuple oid column id
			ULONG m_ulTupleOid;
			
			// direct dispatch info for insert statements 
			CDXLDirectDispatchInfo *m_pdxlddinfo;
			
			// private copy ctor
			CDXLPhysicalDML(const CDXLPhysicalDML &);
			
		public:
			
			// ctor
			CDXLPhysicalDML
				(
				IMemoryPool *pmp,
				const EdxlDmlType edxldmltype,
				CDXLTableDescr *pdxltabdesc,
				DrgPul *pdrgpul,
				ULONG ulAction,
				ULONG ulOid,
				ULONG ulCtid,
				ULONG ulSegmentId,
				BOOL fPreserveOids,
				ULONG ulTupleOid,
				CDXLDirectDispatchInfo *pdxlddinfo
				);

			// dtor
			virtual
			~CDXLPhysicalDML();
		
			// operator type
			Edxlopid Edxlop() const;

			// operator name
			const CWStringConst *PstrOpName() const;

			// DML operator type
			EdxlDmlType EdxlDmlOpType() const
			{
				return m_edxldmltype;
			}

			// target table descriptor 
			CDXLTableDescr *Pdxltabdesc() const
			{
				return m_pdxltabdesc;
			}
			
			// source column ids
			DrgPul *Pdrgpul() const
			{
				return m_pdrgpul;
			}

			// action column id
			ULONG UlAction() const
			{
				return m_ulAction;
			}

			// oid column id
			ULONG UlOid() const
			{
				return m_ulOid;
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
			
			// direct dispatch info
			CDXLDirectDispatchInfo *Pdxlddinfo() const
			{
				return m_pdxlddinfo;
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
			CDXLPhysicalDML *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopPhysicalDML == pdxlop->Edxlop());

				return dynamic_cast<CDXLPhysicalDML*>(pdxlop);
			}
	};
}

#endif // !GPDXL_CDXLPhysicalDML_H

// EOF
