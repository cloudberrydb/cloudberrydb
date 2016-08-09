//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CIndexQualInfo.h
//
//	@doc:
//		Class providing access to the original index qual expression, its modified
//		version tailored for GPDB, and index strategy
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CIndexQualInfo_H
#define GPDXL_CIndexQualInfo_H

#include "postgres.h"

namespace gpdxl
{

	using namespace gpopt;

	class CDXLNode;

	//---------------------------------------------------------------------------
	//	@class:
	//		CIndexQualInfo
	//
	//	@doc:
	//		Class providing access to the original index qual expression, its modified
	//		version tailored for GPDB, and index strategy
	//
	//---------------------------------------------------------------------------
	class CIndexQualInfo
	{
		public:

			// attribute number in the index
			AttrNumber m_attno;

			// index qual expression tailored for GPDB
			Expr *m_pexpr;

			// original index qual expression
			Expr *m_pexprOriginal;

			// index strategy information
			StrategyNumber m_sn;

			// index subtype
			OID m_oidIndexSubtype;
			
			// ctor
			CIndexQualInfo
				(
				AttrNumber attno,
				Expr *pexpr,
				Expr *pexprOriginal,
				StrategyNumber sn,
				OID oidIndexSubtype
				)
				:
				m_attno(attno),
				m_pexpr(pexpr),
				m_pexprOriginal(pexprOriginal),
				m_sn(sn),
				m_oidIndexSubtype(oidIndexSubtype)
				{
					GPOS_ASSERT((IsA(m_pexpr, OpExpr) && IsA(m_pexprOriginal, OpExpr)) ||
						(IsA(m_pexpr, ScalarArrayOpExpr) && IsA(m_pexprOriginal, ScalarArrayOpExpr)));
				}

				// dtor
				~CIndexQualInfo()
				{}

				// comparison function for sorting index qualifiers
				static
				INT IIndexQualInfoCmp
					(
					const void *pv1,
					const void *pv2
					)
				{
					const CIndexQualInfo *pidxqualinfo1 = *(const CIndexQualInfo **) pv1;
					const CIndexQualInfo *pidxqualinfo2 = *(const CIndexQualInfo **) pv2;

					return (INT) pidxqualinfo1->m_attno - (INT) pidxqualinfo2->m_attno;
				}
	};
	// array of index qual info
	typedef CDynamicPtrArray<CIndexQualInfo, CleanupDelete> DrgPindexqualinfo;
}

#endif // !GPDXL_CIndexQualInfo_H

// EOF
