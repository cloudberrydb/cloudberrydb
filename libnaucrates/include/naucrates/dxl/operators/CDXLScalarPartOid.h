//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CDXLScalarPartOid.h
//
//	@doc:
//		Class for representing DXL Part Oid expressions
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarPartOid_H
#define GPDXL_CDXLScalarPartOid_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"

namespace gpdxl
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarPartOid
	//
	//	@doc:
	//		Class for representing DXL Part Oid expressions
	//		These oids are created and consumed by the PartitionSelector operator
	//
	//---------------------------------------------------------------------------
	class CDXLScalarPartOid : public CDXLScalar
	{
		private:

			// partitioning level
			ULONG m_ulLevel;

			// private copy ctor
			CDXLScalarPartOid(const CDXLScalarPartOid&);

		public:
			// ctor
			CDXLScalarPartOid(IMemoryPool *pmp, ULONG ulLevel);

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

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// does the operator return a boolean result
			virtual
			BOOL FBoolean
					(
					CMDAccessor * //pmda
					)
					const
			{
				return false;
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			virtual
			void AssertValid(const CDXLNode *pdxln, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG

			// conversion function
			static
			CDXLScalarPartOid *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarPartOid == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarPartOid*>(pdxlop);
			}
	};
}

#endif // !GPDXL_CDXLScalarPartOid_H

// EOF
