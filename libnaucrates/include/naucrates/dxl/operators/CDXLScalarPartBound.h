//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CDXLScalarPartBound.h
//
//	@doc:
//		Class for representing DXL Part boundary expressions
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarPartBound_H
#define GPDXL_CDXLScalarPartBound_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"

namespace gpdxl
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarPartBound
	//
	//	@doc:
	//		Class for representing DXL Part boundary expressions
	//		These expressions are created and consumed by the PartitionSelector operator
	//
	//---------------------------------------------------------------------------
	class CDXLScalarPartBound : public CDXLScalar
	{
		private:

			// partitioning level
			ULONG m_ulLevel;

			// boundary type
			IMDId *m_pmdidType;

			// whether this represents a lower or upper bound
			BOOL m_fLower;

			// private copy ctor
			CDXLScalarPartBound(const CDXLScalarPartBound&);

		public:
			// ctor
			CDXLScalarPartBound(IMemoryPool *pmp, ULONG ulLevel, IMDId *pmdidType, BOOL fLower);

			// dtor
			virtual
			~CDXLScalarPartBound();

			// operator type
			virtual
			Edxlopid Edxlop() const;

			// operator name
			virtual
			const CWStringConst *PstrOpName() const;

			// partitioning level
			ULONG UlLevel() const
			{
				return m_ulLevel;
			}

			// boundary type
			IMDId *PmdidType() const
			{
				return m_pmdidType;
			}

			// is this a lower (or upper) bound
			BOOL FLower() const
			{
				return m_fLower;
			}

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// does the operator return a boolean result
			virtual
			BOOL FBoolean(CMDAccessor *pmda) const;

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			virtual
			void AssertValid(const CDXLNode *pdxln, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG

			// conversion function
			static
			CDXLScalarPartBound *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarPartBound == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarPartBound*>(pdxlop);
			}
	};
}

#endif // !GPDXL_CDXLScalarPartBound_H

// EOF
