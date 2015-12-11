//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLPhysicalRowTrigger.h
//
//	@doc:
//		Class for representing physical row trigger operator
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLPhysicalRowTrigger_H
#define GPDXL_CDXLPhysicalRowTrigger_H

#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"

#include "naucrates/dxl/operators/CDXLPhysical.h"
#include "naucrates/md/IMDId.h"

using gpmd::IMDId;

namespace gpdxl
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLPhysicalRowTrigger
	//
	//	@doc:
	//		Class for representing physical row trigger operators
	//
	//---------------------------------------------------------------------------
	class CDXLPhysicalRowTrigger : public CDXLPhysical
	{
		private:

			// relation id on which triggers are to be executed
			IMDId *m_pmdidRel;

			// trigger type
			INT m_iType;

			// old column ids
			DrgPul *m_pdrgpulOld;

			// new column ids
			DrgPul *m_pdrgpulNew;

			// private copy ctor
			CDXLPhysicalRowTrigger(const CDXLPhysicalRowTrigger &);

		public:

			// ctor
			CDXLPhysicalRowTrigger
				(
				IMemoryPool *pmp,
				IMDId *pmdidRel,
				INT iType,
				DrgPul *pdrgpulOld,
				DrgPul *pdrgpulNew
				);

			// dtor
			virtual
			~CDXLPhysicalRowTrigger();

			// operator type
			virtual
			Edxlopid Edxlop() const;

			// operator name
			virtual
			const CWStringConst *PstrOpName() const;

			// relation id
			IMDId *PmdidRel() const
			{
				return m_pmdidRel;
			}

			// trigger type
			INT IType() const
			{
				return m_iType;
			}

			// old column ids
			DrgPul *PdrgpulOld() const
			{
				return m_pdrgpulOld;
			}

			// new column ids
			DrgPul *PdrgpulNew() const
			{
				return m_pdrgpulNew;
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
			CDXLPhysicalRowTrigger *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopPhysicalRowTrigger == pdxlop->Edxlop());

				return dynamic_cast<CDXLPhysicalRowTrigger*>(pdxlop);
			}
	};
}

#endif // !GPDXL_CDXLPhysicalRowTrigger_H

// EOF
