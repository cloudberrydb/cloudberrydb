//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLScalarWindowRef.h
//
//	@doc:
//		Class for representing DXL scalar WindowRef
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarWindowRef_H
#define GPDXL_CDXLScalarWindowRef_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"
#include "naucrates/md/IMDId.h"

namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;

	// stage of the evaluation of the window function
	enum EdxlWinStage
	{
		EdxlwinstageImmediate = 0,
		EdxlwinstagePreliminary,
		EdxlwinstageRowKey,
		EdxlwinstageSentinel
	};

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarWindowRef
	//
	//	@doc:
	//		Class for representing DXL scalar WindowRef
	//
	//---------------------------------------------------------------------------
	class CDXLScalarWindowRef : public CDXLScalar
	{
		private:

			// catalog id of the function
			IMDId *m_pmdidFunc;

			// return type
			IMDId *m_pmdidRetType;

			// denotes whether it's agg(DISTINCT ...)
			BOOL m_fDistinct;

			// is argument list really '*' //
			BOOL m_fStarArg;

			// is function a simple aggregate? //
			BOOL m_fSimpleAgg;

			// denotes the win stage
			EdxlWinStage m_edxlwinstage;

			// position the window specification in a parent window operator
			ULONG m_ulWinspecPos;

			// private copy ctor
			CDXLScalarWindowRef(const CDXLScalarWindowRef&);

		public:
			// ctor
			CDXLScalarWindowRef
				(
				IMemoryPool *pmp,
				IMDId *pmdidWinfunc,
				IMDId *pmdidRetType,
				BOOL fDistinct,
				BOOL fStarArg,
				BOOL fSimpleAgg,
				EdxlWinStage edxlwinstage,
				ULONG ulWinspecPosition
				);

			//dtor
			virtual
			~CDXLScalarWindowRef();

			// ident accessors
			Edxlopid Edxlop() const;

			// name of the DXL operator
			const CWStringConst *PstrOpName() const;

			// catalog id of the function
			IMDId *PmdidFunc() const
			{
				return m_pmdidFunc;
			}

			// return type of the function
			IMDId *PmdidRetType() const
			{
				return m_pmdidRetType;
			}

			// window stage
			EdxlWinStage Edxlwinstage() const
			{
				return m_edxlwinstage;
			}

			// denotes whether it's agg(DISTINCT ...)
			BOOL FDistinct() const
			{
				return m_fDistinct;
			}
		
			BOOL FStarArg() const
			{
				return m_fStarArg;
			}

			BOOL FSimpleAgg() const
			{
				return m_fSimpleAgg;
			}

			// position the window specification in a parent window operator
			ULONG UlWinSpecPos() const
			{
				return m_ulWinspecPos;
			}

			// set window spec position
			void SetWinSpecPos
				(
				ULONG ulWinspecPos
				)
			{
				m_ulWinspecPos = ulWinspecPos;
			}

			// string representation of win stage
			const CWStringConst *PstrWinStage() const;

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLScalarWindowRef *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarWindowRef == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarWindowRef*>(pdxlop);
			}

			// does the operator return a boolean result
			virtual
			BOOL FBoolean(CMDAccessor *pmda) const;

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *pdxln, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG
	};
}

#endif // !GPDXL_CDXLScalarWindowRef_H

// EOF

