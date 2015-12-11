//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CDXLScalarBitmapIndexProbe.h
//
//	@doc:
//		Class for representing DXL bitmap index probe operators
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarBitmapIndexProbe_H
#define GPDXL_CDXLScalarBitmapIndexProbe_H

#include "gpos/base.h"

#include "naucrates/dxl/operators/CDXLScalar.h"

namespace gpdxl
{
	using namespace gpos;

	// fwd declarations
	class CDXLIndexDescr;
	class CXMLSerializer;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarBitmapIndexProbe
	//
	//	@doc:
	//		Class for representing DXL bitmap index probe operators
	//
	//---------------------------------------------------------------------------
	class CDXLScalarBitmapIndexProbe : public CDXLScalar
	{
		private:
			// index descriptor associated with the scanned table
			CDXLIndexDescr *m_pdxlid;

			// disable copy ctor
			CDXLScalarBitmapIndexProbe(CDXLScalarBitmapIndexProbe &);

		public:
			// ctor
			CDXLScalarBitmapIndexProbe
				(
				IMemoryPool *pmp,
				CDXLIndexDescr *pdxlid
				);

			//dtor
			virtual
			~CDXLScalarBitmapIndexProbe();

			// operator type
			virtual
			Edxlopid Edxlop() const
			{
				return EdxlopScalarBitmapIndexProbe;
			}

			// operator name
			virtual
			const CWStringConst *PstrOpName() const;

			// index descriptor
			virtual
			const CDXLIndexDescr *Pdxlid() const
			{
				return m_pdxlid;
			}

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// does the operator return a boolean result
			virtual
			BOOL FBoolean
					(
					CMDAccessor *//pmda
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
			CDXLScalarBitmapIndexProbe *PdxlopConvert
				(
					CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarBitmapIndexProbe== pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarBitmapIndexProbe *>(pdxlop);
			}

	};  // class CDXLScalarBitmapIndexProbe
}

#endif  // !GPDXL_CDXLScalarBitmapIndexProbe_H

// EOF
